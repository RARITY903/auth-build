import discord
from discord import app_commands
from discord.ext import commands
import os
import asyncio
from typing import Optional
from datetime import datetime
from config import DISCORD_TOKEN, ADMIN_USER_IDS, EMBED_COLOR
from database import AuthDatabase, DatabaseError
from loader_generator import LoaderGenerator, LoaderGenerationError
from utils import (
    validate_product_name, validate_version_string, validate_license_key,
    is_admin, format_error_message, sanitize_input, validate_discord_id
)
from logger import log

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

class AuthBot(commands.Bot):
    """Main Discord bot for authentication and loader distribution"""
    
    def __init__(self):
        super().__init__(command_prefix='!', intents=intents)
        self.db: Optional[AuthDatabase] = None
        self.loader_gen: Optional[LoaderGenerator] = None
        self._initialize_components()
    
    def is_user_admin(self, user_id: int) -> bool:
        """Check if user is an admin (checks both .env and database)"""
        # Check .env list
        if user_id in ADMIN_USER_IDS:
            return True
        # Check database
        if self.db and self.db.is_admin_db(user_id):
            return True
        return False
    
    def _initialize_components(self):
        """Initialize database and loader generator"""
        try:
            self.db = AuthDatabase()
            self.loader_gen = LoaderGenerator()
            
            # Add owner as initial admin if not already in database
            owner_id = 1438665477843976394
            if not self.db.is_admin_db(owner_id):
                self.db.add_admin(owner_id, owner_id)
                log.info(f"Added owner {owner_id} as initial admin")
            
            log.info("Bot components initialized successfully")
        except Exception as e:
            log.critical(f"Failed to initialize bot components: {e}", exc_info=True)
            raise
    
    async def setup_hook(self):
        """Setup bot hooks and sync commands"""
        try:
            await self.tree.sync()
            log.info("Slash commands synced successfully")
        except Exception as e:
            log.error(f"Failed to sync slash commands: {e}", exc_info=True)
    
    async def on_ready(self):
        """Called when bot is ready"""
        log.info(f'Logged in as {self.user.name} (ID: {self.user.id})')
        log.info(f'Connected to {len(self.guilds)} guilds')
        
        # Set bot status
        try:
            await self.change_presence(
                activity=discord.Activity(
                    type=discord.ActivityType.watching,
                    name="for loader redemptions"
                )
            )
        except Exception as e:
            log.error(f"Failed to set bot presence: {e}", exc_info=True)
    
    async def on_guild_join(self, guild: discord.Guild):
        """Called when bot joins a guild"""
        log.info(f"Joined new guild: {guild.name} (ID: {guild.id})")
    
    async def on_guild_remove(self, guild: discord.Guild):
        """Called when bot leaves a guild"""
        log.info(f"Left guild: {guild.name} (ID: {guild.id})")
    
    async def close(self):
        """Clean shutdown"""
        log.info("Shutting down bot...")
        if self.db:
            self.db.close()
        await super().close()

bot = AuthBot()

@bot.event
async def on_interaction(interaction: discord.Interaction):
    """Handle all interactions"""
    try:
        # Handle button interactions
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get('custom_id', '')
            log.debug(f"Button interaction received: {custom_id}")
            
            if custom_id == 'redeem_loader':
                await handle_redeem_button(interaction)
    except Exception as e:
        log.error(f"Error handling interaction: {e}", exc_info=True)
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "An error occurred while processing your request.",
                    ephemeral=True
                )
        except:
            pass

async def handle_redeem_button(interaction: discord.Interaction):
    """Handle the redeem loader button click"""
    try:
        if interaction.response.is_done():
            # If interaction was already acknowledged, use followup
            modal = KeyInputModal(title="Redeem Your Loader")
            await interaction.followup.send_modal(modal)
        else:
            # If not acknowledged, use response
            modal = KeyInputModal(title="Redeem Your Loader")
            await interaction.response.send_modal(modal)
        log.debug(f"Redeem modal sent to user {interaction.user.id}")
    except discord.HTTPException as e:
        if "already been acknowledged" in str(e):
            log.debug(f"Interaction already acknowledged for user {interaction.user.id}")
            # Try to send a message instead
            try:
                await interaction.followup.send(
                    "Please click the button again to redeem your loader.",
                    ephemeral=True
                )
            except:
                pass
        else:
            log.error(f"Failed to send redeem modal: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Failed to open redemption form. Please try again.",
                    ephemeral=True
                )
    except Exception as e:
        log.error(f"Failed to send redeem modal: {e}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "Failed to open redemption form. Please try again.",
                ephemeral=True
            )

class KeyInputModal(discord.ui.Modal, title='Enter Your Key'):
    """Modal for users to input their license key"""
    
    key = discord.ui.TextInput(
        label='License Key',
        placeholder='XXXX-XXXX-XXXX-XXXX',
        style=discord.TextStyle.short,
        required=True,
        max_length=19,
        min_length=19
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle modal submission - show product selection"""
        await interaction.response.defer(ephemeral=True)
        
        key = self.key.value.strip().upper()
        
        log.info(f"Key entered by user {interaction.user.id}: {key[:8]}...")
        
        # Validate key format
        if not validate_license_key(key):
            await interaction.followup.send(
                "❌ Invalid key format. Expected format: XXXX-XXXX-XXXX-XXXX",
                ephemeral=True
            )
            return
        
        # Validate the key and get the product
        try:
            is_valid, valid_product = bot.db.validate_key(key)
            
            if not is_valid:
                log.warning(f"Invalid key attempted by user {interaction.user.id}: {key[:8]}...")
                await interaction.followup.send(
                    "❌ Invalid or already used key. Please check your key and try again.",
                    ephemeral=True
                )
                return
            
            # Show product selection view
            view = ProductSelectionView(key, valid_product)
            embed = discord.Embed(
                title="🔑 Select Product",
                description=f"Your key is valid for: **{valid_product}**\n\nClick the button below to redeem your loader.",
                color=EMBED_COLOR
            )
            embed.set_footer(text="Click 'Redeem Loader' to continue")
            
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            
        except Exception as e:
            log.error(f"Error validating key: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while validating your key. Please try again later.",
                ephemeral=True
            )

class ProductSelectionView(discord.ui.View):
    """View for product selection during redemption"""
    
    def __init__(self, key: str, valid_product: str):
        super().__init__(timeout=180)  # 3 minutes timeout
        self.key = key
        self.valid_product = valid_product
    
    @discord.ui.button(label='Redeem Loader', style=discord.ButtonStyle.primary, emoji='🎉')
    async def redeem_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle the final redemption"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            user_id = interaction.user.id
            unique_hash = bot.loader_gen.generate_unique_hash(user_id, self.valid_product)
            
            # Get product info for template
            product_info = bot.db.get_product(self.valid_product)
            template_path = product_info[2] if product_info else None
            version = product_info[1] if product_info else "1.0.0"
            
            # Create custom loader
            loader_path = bot.loader_gen.create_loader_file(
                self.valid_product, 
                unique_hash, 
                template_path, 
                version
            )
            
            # Mark key as used
            if not bot.db.mark_key_used(self.key, user_id):
                log.error(f"Failed to mark key as used for user {user_id}")
                await interaction.followup.send(
                    "❌ An error occurred while processing your key. Please contact support.",
                    ephemeral=True
                )
                return
            
            # Get key ID for database record
            key_id = bot.db.get_key_id(self.key)
            if not key_id:
                log.error(f"Failed to get key ID after marking as used")
                await interaction.followup.send(
                    "❌ An error occurred while processing your key. Please contact support.",
                    ephemeral=True
                )
                return
            
            # Save loader to database
            if not bot.db.save_loader(key_id, self.valid_product, unique_hash, loader_path, version):
                log.error(f"Failed to save loader to database")
                await interaction.followup.send(
                    "❌ An error occurred while saving your loader. Please contact support.",
                    ephemeral=True
                )
                return
            
            # Send DM with loader
            try:
                embed = discord.Embed(
                    title=f"🎉 {self.valid_product} Loader Generated!",
                    description=f"Your custom loader has been generated successfully!",
                    color=EMBED_COLOR
                )
                embed.add_field(name="Unique Hash", value=f"`{unique_hash}`", inline=False)
                embed.add_field(name="Version", value=version, inline=True)
                embed.add_field(name="Generated For", value=f"<@{user_id}>", inline=True)
                embed.add_field(name="Generated At", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"), inline=True)
                embed.set_footer(text="⚠️ Keep this file safe - do not share it!")
                
                file = discord.File(loader_path, filename=os.path.basename(loader_path))
                
                await interaction.user.send(embed=embed, file=file)
                await interaction.followup.send(
                    "✅ Your loader has been sent to your DMs! Check your private messages.",
                    ephemeral=True
                )
                log.info(f"Successfully sent loader to user {user_id} for product {self.valid_product}")
                
                # Disable the button after successful redemption
                button.disabled = True
                button.label = "Already Redeemed"
                button.style = discord.ButtonStyle.secondary
                await interaction.edit_original_response(view=self)
                
            except discord.Forbidden:
                log.warning(f"DM blocked for user {user_id}")
                await interaction.followup.send(
                    "❌ I couldn't send you a DM. Please enable DMs from server members and try again.",
                    ephemeral=True
                )
            except Exception as e:
                log.error(f"Error sending DM to user {user_id}: {e}", exc_info=True)
                await interaction.followup.send(
                    "❌ An error occurred while sending your loader. Please contact support.",
                    ephemeral=True
                )
        except LoaderGenerationError as e:
            log.error(f"Loader generation error for user {interaction.user.id}: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while generating your loader. Please contact support.",
                ephemeral=True
            )
        except Exception as e:
            log.error(f"Unexpected error during redemption for user {interaction.user.id}: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An unexpected error occurred. Please contact support.",
                ephemeral=True
            )

# Admin Commands
@bot.tree.command(name='send_redeem_embed', description='Send a redeem embed to a channel')
@app_commands.describe(channel='The channel to send the embed to')
async def send_redeem_embed(interaction: discord.Interaction, channel: discord.TextChannel):
    """Send a redeem embed with a button to a specified channel"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        # Check bot permissions in the channel
        bot_permissions = channel.permissions_for(interaction.guild.me)
        if not bot_permissions.send_messages or not bot_permissions.embed_links:
            await interaction.response.send_message(
                "❌ I don't have permission to send messages or embeds in that channel.",
                ephemeral=True
            )
            return
        
        embed = discord.Embed(
            title="🔑 Redeem Your Loader",
            description="Click the button below to redeem your purchased license key and receive your custom loader.",
            color=EMBED_COLOR
        )
        embed.add_field(
            name="How to Redeem",
            value="1. Click the 'Redeem Loader' button below\n2. Enter your purchased license key\n3. Select your product\n4. Receive your custom loader via DM",
            inline=False
        )
        embed.set_footer(text="Support: Contact staff if you have issues")
        embed.set_thumbnail(url=interaction.guild.icon.url if interaction.guild.icon else None)
        
        view = RedeemView()
        
        await channel.send(embed=embed, view=view)
        await interaction.response.send_message(f"✅ Redeem embed sent to {channel.mention}", ephemeral=True)
        log.info(f"Redeem embed sent to channel {channel.id} by admin {interaction.user.id}")
    except Exception as e:
        log.error(f"Error sending redeem embed: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "sending redeem embed"),
            ephemeral=True
        )


class RedeemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label='Redeem Loader', style=discord.ButtonStyle.primary, emoji='🔑', custom_id='redeem_loader')
    async def redeem_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_redeem_button(interaction)

async def product_autocomplete(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
    """Autocomplete function for product names"""
    try:
        products = bot.db.get_all_products()
        choices = []
        
        for product_name, version in products:
            if current.lower() in product_name.lower():
                choices.append(app_commands.Choice(name=f"{product_name} (v{version})", value=product_name))
        
        # Limit to 25 choices (Discord limit)
        return choices[:25]
    except Exception as e:
        log.error(f"Error in product autocomplete: {e}", exc_info=True)
        return []

@bot.tree.command(name='generate_key', description='Generate a new license key (Admin only)')
@app_commands.describe(product='The product to generate keys for', count='Number of keys to generate (max 50)')
@app_commands.autocomplete(product=product_autocomplete)
async def generate_key(interaction: discord.Interaction, product: str, count: int = 1):
    """Generate license keys for a product"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        # Validate inputs
        if not validate_product_name(product):
            await interaction.response.send_message(
                "❌ Invalid product name format.",
                ephemeral=True
            )
            return
        
        if count < 1 or count > 50:
            await interaction.response.send_message(
                "❌ Count must be between 1 and 50.",
                ephemeral=True
            )
            return
        
        import secrets
        import string
        
        keys_generated = []
        for _ in range(count):
            # Generate a random key
            key = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(16))
            key = f"{key[:4]}-{key[4:8]}-{key[8:12]}-{key[12:]}"
            
            # Add to database
            if bot.db.add_key(key, product):
                keys_generated.append(key)
        
        if keys_generated:
            embed = discord.Embed(
                title=f"🔑 Generated {len(keys_generated)} Key(s)",
                color=EMBED_COLOR
            )
            
            keys_text = '\n'.join([f"`{k}`" for k in keys_generated])
            if len(keys_text) > 1000:
                keys_text = '\n'.join([f"`{k}`" for k in keys_generated[:10]]) + f"\n... and {len(keys_generated) - 10} more"
            
            embed.add_field(name="Keys", value=keys_text, inline=False)
            embed.add_field(name="Product", value=product, inline=False)
            embed.set_footer(text=f"Generated by {interaction.user.display_name}")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            log.info(f"Generated {len(keys_generated)} keys for product {product} by admin {interaction.user.id}")
        else:
            await interaction.response.send_message("❌ Failed to generate keys. Product may not exist.", ephemeral=True)
    except Exception as e:
        log.error(f"Error generating keys: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "generating keys"),
            ephemeral=True
        )

@bot.tree.command(name='add_product', description='Add a new product (Admin only)')
@app_commands.describe(name='Product name', template_path='Path to loader template file (optional)')
async def add_product(interaction: discord.Interaction, name: str, template_path: Optional[str] = None):
    """Add a new product to the system"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        # Validate product name
        if not validate_product_name(name):
            await interaction.response.send_message(
                "❌ Invalid product name format. Use only letters, numbers, spaces, hyphens, and underscores.",
                ephemeral=True
            )
            return
        
        # Validate template path if provided
        if template_path and not os.path.exists(template_path):
            await interaction.response.send_message(
                "❌ Template file not found at the specified path.",
                ephemeral=True
            )
            return
        
        if bot.db.add_product(name, template_path):
            await interaction.response.send_message(
                f"✅ Product '{name}' added successfully!",
                ephemeral=True
            )
            log.info(f"Product '{name}' added by admin {interaction.user.id}")
        else:
            await interaction.response.send_message(
                f"❌ Product '{name}' already exists.",
                ephemeral=True
            )
    except Exception as e:
        log.error(f"Error adding product: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "adding product"),
            ephemeral=True
        )

@bot.tree.command(name='upload_loader', description='Upload a new loader version for auto-updates (Admin only)')
@app_commands.describe(product='The product to upload loader for', version='Version string (e.g., 1.0.0)', file='The loader file to upload')
@app_commands.autocomplete(product=product_autocomplete)
async def upload_loader(interaction: discord.Interaction, product: str, version: str, file: discord.Attachment):
    """Upload a new loader version for auto-updates"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    temp_file_path = None
    try:
        # Validate inputs
        if not validate_product_name(product):
            await interaction.response.send_message(
                "❌ Invalid product name format.",
                ephemeral=True
            )
            return
        
        if not validate_version_string(version):
            await interaction.response.send_message(
                "❌ Invalid version format. Use semantic versioning (e.g., 1.0.0).",
                ephemeral=True
            )
            return
        
        # Validate file size (max 25MB)
        if file.size > 25 * 1024 * 1024:
            await interaction.response.send_message(
                "❌ File too large. Maximum size is 25MB.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer(ephemeral=True)
        
        # Download the file
        temp_file_path = f"temp_{file.filename}"
        await file.save(temp_file_path)
        log.info(f"Downloaded file {file.filename} for upload by admin {interaction.user.id}")
        
        # Upload to loader system
        uploaded_path = bot.loader_gen.upload_loader_version(product, version, temp_file_path)
        
        # Update product version in database
        if not bot.db.update_product_version(product, version, uploaded_path):
            await interaction.followup.send(
                "❌ Failed to update product version in database.",
                ephemeral=True
            )
            return
        
        await interaction.followup.send(
            f"✅ Loader for '{product}' version {version} uploaded successfully!",
            ephemeral=True
        )
        log.info(f"Loader uploaded for product {product} version {version} by admin {interaction.user.id}")
    except LoaderGenerationError as e:
        log.error(f"Loader generation error during upload: {e}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "❌ Failed to process loader file.",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                "❌ Failed to process loader file.",
                ephemeral=True
            )
    except Exception as e:
        log.error(f"Error uploading loader: {e}", exc_info=True)
        if not interaction.response.is_done():
            await interaction.response.send_message(
                format_error_message(e, "uploading loader"),
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                format_error_message(e, "uploading loader"),
                ephemeral=True
            )
    finally:
        # Clean up temp file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                log.debug(f"Cleaned up temp file: {temp_file_path}")
            except Exception as e:
                log.warning(f"Failed to clean up temp file {temp_file_path}: {e}")

@bot.tree.command(name='list_products', description='List all available products (Admin only)')
async def list_products(interaction: discord.Interaction):
    """List all available products"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        products = bot.db.get_all_products()
        
        if products:
            embed = discord.Embed(
                title="📦 Available Products",
                color=EMBED_COLOR
            )
            
            for name, version in products:
                embed.add_field(name=name, value=f"Version: {version}", inline=True)
            
            embed.set_footer(text=f"Total: {len(products)} product(s)")
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(
                "No products found. Use /add_product to add one.",
                ephemeral=True
            )
    except Exception as e:
        log.error(f"Error listing products: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "listing products"),
            ephemeral=True
        )

@bot.tree.command(name='stats', description='Show system statistics (Admin only)')
async def stats(interaction: discord.Interaction):
    """Show system statistics"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        # Get statistics from database
        db_stats = bot.db.get_statistics()
        
        # Get loader statistics
        loader_stats = bot.loader_gen.get_loader_stats()
        
        embed = discord.Embed(
            title="📊 System Statistics",
            color=EMBED_COLOR
        )
        embed.add_field(name="Total Keys Generated", value=db_stats.get('total_keys', 0), inline=True)
        embed.add_field(name="Keys Redeemed", value=db_stats.get('used_keys', 0), inline=True)
        embed.add_field(name="Keys Available", value=db_stats.get('total_keys', 0) - db_stats.get('used_keys', 0), inline=True)
        embed.add_field(name="Loaders Generated", value=db_stats.get('total_loaders', 0), inline=True)
        embed.add_field(name="Total Products", value=db_stats.get('total_products', 0), inline=True)
        embed.add_field(name="Loader Files", value=loader_stats.get('total_files', 0), inline=True)
        
        if loader_stats.get('total_size_mb'):
            embed.add_field(name="Total Storage Used", value=f"{loader_stats['total_size_mb']} MB", inline=False)
        
        embed.set_footer(text=f"Requested by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception as e:
        log.error(f"Error getting stats: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "getting statistics"),
            ephemeral=True
        )

@bot.tree.command(name='user_stats', description='View your redemption statistics (Anyone can use)')
async def user_stats(interaction: discord.Interaction):
    """View personal redemption statistics"""
    try:
        user_id = interaction.user.id
        stats = bot.db.get_user_statistics(user_id)
        
        if not stats or stats.get('keys_redeemed', 0) == 0:
            await interaction.response.send_message(
                "You haven't redeemed any keys yet. Use the redeem button to get started!",
                ephemeral=True
            )
            return
        
        embed = discord.Embed(
            title=f"📊 Your Statistics",
            color=EMBED_COLOR
        )
        
        embed.set_thumbnail(url=interaction.user.display_avatar.url if interaction.user.display_avatar else None)
        
        # Basic stats
        embed.add_field(name="Keys Redeemed", value=str(stats['keys_redeemed']), inline=True)
        embed.add_field(name="Loaders Generated", value=str(stats['loaders_generated']), inline=True)
        
        # Time info
        if stats['first_redemption']:
            embed.add_field(name="First Redemption", value=stats['first_redemption'], inline=False)
        if stats['last_redemption']:
            embed.add_field(name="Last Redemption", value=stats['last_redemption'], inline=False)
        
        # Products breakdown
        if stats['products']:
            products_text = "\n".join([f"• {name}: {count}" for name, count in stats['products']])
            embed.add_field(name="Products Redeemed", value=products_text, inline=False)
        
        # Recent activity
        if stats['recent_activity']:
            recent_text = "\n".join([f"• {product}" for product, _ in stats['recent_activity']])
            embed.add_field(name="Recent Activity", value=recent_text, inline=False)
        
        embed.set_footer(text=f"Requested by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Log the action
        bot.db.log_action(user_id, "VIEWED_USER_STATS")
        
    except Exception as e:
        log.error(f"Error getting user statistics: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "getting user statistics"),
            ephemeral=True
        )

@bot.tree.command(name='top_users', description='View top users by keys redeemed (Admin only)')
@app_commands.check(lambda interaction: bot.is_user_admin(interaction.user.id))
async def top_users(interaction: discord.Interaction, limit: int = 10):
    """View top users by number of keys redeemed"""
    try:
        if limit < 1 or limit > 50:
            await interaction.response.send_message(
                "❌ Limit must be between 1 and 50.",
                ephemeral=True
            )
            return
        
        top_users = bot.db.get_top_users(limit)
        
        if not top_users:
            await interaction.response.send_message(
                "No users have redeemed any keys yet.",
                ephemeral=True
            )
            return
        
        embed = discord.Embed(
            title=f"🏆 Top {limit} Users",
            description="Users with the most keys redeemed",
            color=EMBED_COLOR
        )
        
        for i, (user_id, count, _) in enumerate(top_users, 1):
            try:
                user = await bot.fetch_user(user_id)
                username = user.display_name
            except:
                username = f"User {user_id}"
            
            embed.add_field(
                name=f"#{i} {username}",
                value=f"{count} keys redeemed",
                inline=False
            )
        
        embed.set_footer(text=f"Requested by {interaction.user.display_name}")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Log the action
        bot.db.log_action(interaction.user.id, "VIEWED_TOP_USERS", f"limit={limit}")
        
    except Exception as e:
        log.error(f"Error getting top users: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "getting top users"),
            ephemeral=True
        )

@top_users.error
async def top_users_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Error handler for top_users command"""
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    else:
        log.error(f"Command error in top_users: {error}", exc_info=True)

@bot.tree.command(name='bulk_generate_keys', description='Generate bulk keys with CSV export (Admin only)')
@app_commands.describe(product='The product to generate keys for', count='Number of keys to generate (max 1000)', export_csv='Export keys to CSV file')
@app_commands.autocomplete(product=product_autocomplete)
async def bulk_generate_keys(interaction: discord.Interaction, product: str, count: int = 100, export_csv: bool = True):
    """Generate multiple keys in bulk with optional CSV export"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        # Validate inputs
        if not validate_product_name(product):
            await interaction.response.send_message(
                "❌ Invalid product name format.",
                ephemeral=True
            )
            return
        
        if count < 1 or count > 1000:
            await interaction.response.send_message(
                "❌ Count must be between 1 and 1000.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer(ephemeral=True)
        
        # Generate keys
        keys = bot.db.bulk_generate_keys(product, count)
        
        if not keys:
            await interaction.followup.send(
                "❌ Failed to generate keys. Product may not exist.",
                ephemeral=True
            )
            return
        
        embed = discord.Embed(
            title=f"🔑 Bulk Key Generation Complete",
            color=EMBED_COLOR
        )
        embed.add_field(name="Product", value=product, inline=True)
        embed.add_field(name="Keys Generated", value=str(len(keys)), inline=True)
        embed.set_footer(text=f"Generated by {interaction.user.display_name}")
        
        # Create CSV file if requested
        if export_csv and len(keys) > 0:
            import csv
            import io
            
            # Create CSV content
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['Key', 'Product', 'Status'])
            
            for key in keys:
                writer.writerow([key, product, 'Unused'])
            
            csv_content = output.getvalue()
            output.close()
            
            # Create file
            csv_file = discord.File(
                io.BytesIO(csv_content.encode('utf-8')),
                filename=f"{product}_keys_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            )
            
            await interaction.followup.send(
                embed=embed,
                file=csv_file,
                ephemeral=True
            )
        else:
            # Show sample keys if not exporting
            sample_keys = keys[:10]
            keys_text = '\n'.join([f"`{k}`" for k in sample_keys])
            if len(keys) > 10:
                keys_text += f"\n... and {len(keys) - 10} more"
            
            embed.add_field(name="Sample Keys", value=keys_text, inline=False)
            
            await interaction.followup.send(
                embed=embed,
                ephemeral=True
            )
        
        # Log the action
        bot.db.log_action(interaction.user.id, "BULK_KEY_GENERATION", f"product={product}, count={len(keys)}")
        log.info(f"Bulk generated {len(keys)} keys for product {product} by admin {interaction.user.id}")
        
    except Exception as e:
        log.error(f"Error in bulk key generation: {e}", exc_info=True)
        await interaction.followup.send(
            format_error_message(e, "bulk generating keys"),
            ephemeral=True
        )

@bulk_generate_keys.error
async def bulk_generate_keys_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Error handler for bulk_generate_keys command"""
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    else:
        log.error(f"Command error in bulk_generate_keys: {error}", exc_info=True)

@bot.tree.command(name='generate_expiring_keys', description='Generate keys with expiration (Admin only)')
@app_commands.describe(product='The product to generate keys for', count='Number of keys to generate', expires_days='Days until keys expire')
@app_commands.autocomplete(product=product_autocomplete)
async def generate_expiring_keys(interaction: discord.Interaction, product: str, count: int = 100, expires_days: int = 30):
    """Generate keys that expire after specified days"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        # Validate inputs
        if not validate_product_name(product):
            await interaction.response.send_message(
                "❌ Invalid product name format.",
                ephemeral=True
            )
            return
        
        if count < 1 or count > 1000:
            await interaction.response.send_message(
                "❌ Count must be between 1 and 1000.",
                ephemeral=True
            )
            return
        
        if expires_days < 1 or expires_days > 365:
            await interaction.response.send_message(
                "❌ Expiration days must be between 1 and 365.",
                ephemeral=True
            )
            return
        
        await interaction.response.defer(ephemeral=True)
        
        # Generate expiring keys
        keys = bot.db.bulk_generate_keys(product, count, expires_days)
        
        if not keys:
            await interaction.followup.send(
                "❌ Failed to generate keys. Product may not exist.",
                ephemeral=True
            )
            return
        
        embed = discord.Embed(
            title=f"⏰ Expiring Keys Generated",
            color=EMBED_COLOR
        )
        embed.add_field(name="Product", value=product, inline=True)
        embed.add_field(name="Keys Generated", value=str(len(keys)), inline=True)
        embed.add_field(name="Expires In", value=f"{expires_days} days", inline=True)
        embed.add_field(name="Expiration Date", value=f"<t:{int((datetime.now().timestamp() + expires_days * 86400))}:D>", inline=False)
        embed.set_footer(text=f"Generated by {interaction.user.display_name}")
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
        # Log the action
        bot.db.log_action(interaction.user.id, "EXPIRING_KEY_GENERATION", f"product={product}, count={len(keys)}, expires_in={expires_days}")
        log.info(f"Generated {len(keys)} expiring keys for product {product} by admin {interaction.user.id}")
        
    except Exception as e:
        log.error(f"Error generating expiring keys: {e}", exc_info=True)
        await interaction.followup.send(
            format_error_message(e, "generating expiring keys"),
            ephemeral=True
        )

@generate_expiring_keys.error
async def generate_expiring_keys_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Error handler for generate_expiring_keys command"""
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    else:
        log.error(f"Command error in generate_expiring_keys: {error}", exc_info=True)

@bot.tree.command(name='cleanup_expired_keys', description='Mark expired keys as expired (Admin only)')
@app_commands.describe(dry_run='Show what would be marked as expired without actually doing it')
async def cleanup_expired_keys(interaction: discord.Interaction, dry_run: bool = False):
    """Clean up expired keys"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        await interaction.response.defer(ephemeral=True)
        
        if dry_run:
            expiring_keys = bot.db.get_expiring_keys(0)  # Get already expired keys
            count = len(expiring_keys)
            
            embed = discord.Embed(
                title="🔍 Expired Keys (Dry Run)",
                color=EMBED_COLOR
            )
            embed.add_field(name="Keys to Mark as Expired", value=str(count), inline=True)
            embed.set_footer(text="Dry run - no changes made")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            count = bot.db.cleanup_expired_keys()
            
            embed = discord.Embed(
                title="🧹 Cleanup Complete",
                color=EMBED_COLOR
            )
            embed.add_field(name="Keys Marked as Expired", value=str(count), inline=True)
            embed.set_footer(text=f"Cleanup by {interaction.user.display_name}")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Log the action
            bot.db.log_action(interaction.user.id, "CLEANUP_EXPIRED_KEYS", f"cleaned={count}")
            log.info(f"Cleaned up {count} expired keys by admin {interaction.user.id}")
        
    except Exception as e:
        log.error(f"Error cleaning up expired keys: {e}", exc_info=True)
        await interaction.followup.send(
            format_error_message(e, "cleaning up expired keys"),
            ephemeral=True
        )

@cleanup_expired_keys.error
async def cleanup_expired_keys_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Error handler for cleanup_expired_keys command"""
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    else:
        log.error(f"Command error in cleanup_expired_keys: {error}", exc_info=True)

@bot.tree.command(name='expiring_keys', description='View keys expiring soon (Admin only)')
@app_commands.describe(days='Days ahead to check for expiring keys')
async def expiring_keys(interaction: discord.Interaction, days: int = 7):
    """View keys that will expire soon"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        if days < 1 or days > 30:
            await interaction.response.send_message(
                "❌ Days must be between 1 and 30.",
                ephemeral=True
            )
            return
        
        expiring_keys = bot.db.get_expiring_keys(days)
        
        if not expiring_keys:
            embed = discord.Embed(
                title="📅 No Expiring Keys",
                description=f"No keys expiring in the next {days} days.",
                color=EMBED_COLOR
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title=f"⏰ Keys Expiring in {days} Days",
            color=EMBED_COLOR
        )
        
        # Group by product
        products = {}
        for key, product, expires_at in expiring_keys:
            if product not in products:
                products[product] = []
            products[product].append((key, expires_at))
        
        for product, keys_list in products.items():
            keys_text = f"**{len(keys_list)} key(s)**\n"
            for key, expires_at in keys_list[:5]:  # Show max 5 per product
                keys_text += f"• `{key[:8]}...` - <t:{int(datetime.strptime(expires_at, '%Y-%m-%d %H:%M:%S').timestamp())}:R>\n"
            
            if len(keys_list) > 5:
                keys_text += f"... and {len(keys_list) - 5} more"
            
            embed.add_field(name=product, value=keys_text, inline=False)
        
        embed.set_footer(text=f"Total: {len(expiring_keys)} expiring keys")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Log the action
        bot.db.log_action(interaction.user.id, "VIEWED_EXPIRING_KEYS", f"days={days}")
        
    except Exception as e:
        log.error(f"Error viewing expiring keys: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "viewing expiring keys"),
            ephemeral=True
        )

@expiring_keys.error
async def expiring_keys_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    """Error handler for expiring_keys command"""
    if isinstance(error, app_commands.CheckFailure):
        await interaction.response.send_message(
            "❌ You don't have permission to use this command.",
            ephemeral=True
        )
    else:
        log.error(f"Command error in expiring_keys: {error}", exc_info=True)

@bot.tree.command(name='add_admin', description='Add a user to the admin list (Admin only)')
@app_commands.describe(user='The user to add as admin')
async def add_admin(interaction: discord.Interaction, user: discord.User):
    """Add a user to the admin list"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        if not validate_discord_id(str(user.id)):
            await interaction.response.send_message("❌ Invalid Discord user ID.", ephemeral=True)
            return
        
        if bot.db.add_admin(user.id, interaction.user.id):
            await interaction.response.send_message(
                f"✅ {user.mention} has been added to the admin list.",
                ephemeral=True
            )
            log.info(f"Admin {interaction.user.id} added {user.id} to admin list")
        else:
            await interaction.response.send_message(
                f"❌ {user.mention} is already an admin.",
                ephemeral=True
            )
    except Exception as e:
        log.error(f"Error adding admin: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "adding admin"),
            ephemeral=True
        )

@bot.tree.command(name='remove_admin', description='Remove a user from the admin list (Admin only)')
@app_commands.describe(user='The user to remove from admin')
async def remove_admin(interaction: discord.Interaction, user: discord.User):
    """Remove a user from the admin list"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        if user.id == interaction.user.id:
            await interaction.response.send_message(
                "❌ You cannot remove yourself from the admin list.",
                ephemeral=True
            )
            return
        
        if bot.db.remove_admin(user.id):
            await interaction.response.send_message(
                f"✅ {user.mention} has been removed from the admin list.",
                ephemeral=True
            )
            log.info(f"Admin {interaction.user.id} removed {user.id} from admin list")
        else:
            await interaction.response.send_message(
                f"❌ {user.mention} is not an admin.",
                ephemeral=True
            )
    except Exception as e:
        log.error(f"Error removing admin: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "removing admin"),
            ephemeral=True
        )

@bot.tree.command(name='list_admins', description='List all admins (Admin only)')
async def list_admins(interaction: discord.Interaction):
    """List all admins"""
    if not bot.is_user_admin(interaction.user.id):
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return
    
    try:
        env_admins = ADMIN_USER_IDS
        db_admins = bot.db.get_all_admins()
        
        all_admins = list(set(env_admins + db_admins))
        
        if all_admins:
            embed = discord.Embed(
                title="👑 Admin List",
                color=EMBED_COLOR
            )
            
            admin_mentions = []
            for admin_id in all_admins:
                try:
                    admin_user = await bot.fetch_user(admin_id)
                    admin_mentions.append(f"{admin_user.mention} ({admin_id})")
                except:
                    admin_mentions.append(f"<@{admin_id}> ({admin_id})")
            
            embed.description = "\n".join(admin_mentions)
            embed.set_footer(text=f"Total: {len(all_admins)} admin(s)")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message("No admins found.", ephemeral=True)
    except Exception as e:
        log.error(f"Error listing admins: {e}", exc_info=True)
        await interaction.response.send_message(
            format_error_message(e, "listing admins"),
            ephemeral=True
        )

if __name__ == '__main__':
    try:
        if not DISCORD_TOKEN:
            log.critical("DISCORD_TOKEN not found in environment variables!")
            print("ERROR: DISCORD_TOKEN not found in environment variables!")
            print("Please create a .env file with your Discord bot token.")
            exit(1)
        
        log.info("Starting Discord bot...")
        bot.run(DISCORD_TOKEN)
    except KeyboardInterrupt:
        log.info("Bot shutdown requested by user")
    except Exception as e:
        log.critical(f"Fatal error starting bot: {e}", exc_info=True)
        print(f"ERROR: {e}")
        exit(1)
