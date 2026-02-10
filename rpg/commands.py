# rpg/commands.py
import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
import random

class RPGCommands(commands.Cog):
    def __init__(self, bot, rpg_system):
        self.bot = bot
        self.rpg = rpg_system
    
    # ========== RPG 核心指令 ==========
    
    @app_commands.command(name="rpg_start", description="開始 RPG 冒險")
    @app_commands.describe(nickname="角色暱稱（可選）")
    async def rpg_start_slash(self, interaction: discord.Interaction, nickname: str = None):
        """開始 RPG 冒險"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            user_id = interaction.user.id
            user_name = interaction.user.display_name
            nickname = nickname or user_name
            
            # 使用 RPG 系統創建角色
            success = await self.rpg.player.create_simple_player(user_id, nickname)
            
            if success:
                embed = discord.Embed(
                    title="🎉 RPG 冒險開始！",
                    description=f"歡迎 **{nickname}** 來到阿爾比恩大陸！",
                    color=0x00FF00
                )
                
                embed.add_field(
                    name="👤 角色資訊",
                    value=(
                        f"**冒險者：** {nickname}\n"
                        f"**初始等級：** 1\n"
                        f"**初始HP：** 100\n"
                        f"**初始MP：** 50\n"
                        f"**所屬：** 小雲孤兒院"
                    ),
                    inline=False
                )
                
                embed.add_field(
                    name="🎯 新手任務",
                    value=(
                        "1. 查看狀態 `/rpg_status`\n"
                        "2. 查看背包 `/rpg_inventory`\n"
                        "3. 開始冒險 `/rpg_explore`"
                    ),
                    inline=False
                )
                
                if interaction.user.avatar:
                    embed.set_thumbnail(url=interaction.user.avatar.url)
                
                await interaction.followup.send(embed=embed, ephemeral=False)
            else:
                embed = discord.Embed(
                    title="❌ 創建角色失敗",
                    description="創建過程中發生錯誤，請稍後再試。",
                    color=0xFF0000
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ RPG 創建失敗",
                description=f"錯誤：{str(e)}",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
    
    @app_commands.command(name="rpg_status", description="查看角色狀態")
    async def rpg_status_slash(self, interaction: discord.Interaction):
        """查看角色狀態 - 修復版"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            user_id = interaction.user.id
            
            # 使用 RPG 系統獲取角色
            player = await self.rpg.player.get_player_safe(user_id)
            
            if not player:
                # 自動創建角色
                success = await self.rpg.player.create_simple_player(user_id, interaction.user.display_name)
                
                if not success:
                    embed = discord.Embed(
                        title="❌ 角色不存在",
                        description="請先使用 `/rpg_start` 創建角色",
                        color=0xFF0000
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    return
                
                # 重新獲取玩家資料
                player = await self.rpg.player.get_player_safe(user_id)
            
            # 顯示狀態
            embed = await self.rpg.player.create_status_embed(interaction.user, player)
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ 讀取角色狀態失敗",
                description=f"錯誤：{str(e)}",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
    
    # ========== RPG 修復指令 ==========
    
    @app_commands.command(name="rpg_fix", description="修復 RPG 角色資料庫問題")
    async def rpg_fix_slash(self, interaction: discord.Interaction):
        """修復 RPG 角色"""
        await interaction.response.defer(ephemeral=True)
        
        if interaction.user.id not in [337237662157242368]:  # 你的 ID
            await interaction.followup.send("❌ 只有機器人擁有者可以使用此指令！", ephemeral=True)
            return
        
        try:
            user_id = interaction.user.id
            
            embed = discord.Embed(
                title="🔧 RPG 資料庫修復工具",
                color=0x7289DA
            )
            
            class FixView(discord.ui.View):
                def __init__(self, rpg_system, user_id):
                    super().__init__(timeout=60)
                    self.rpg = rpg_system
                    self.user_id = user_id
                
                @discord.ui.button(label="刪除並重建角色", style=discord.ButtonStyle.danger, emoji="🗑️")
                async def delete_and_recreate(self, interaction: discord.Interaction, button: discord.ui.Button):
                    if interaction.user.id != self.user_id:
                        await interaction.response.send_message("❌ 這不是你的操作！", ephemeral=True)
                        return
                    
                    await interaction.response.defer(ephemeral=True)
                    
                    try:
                        # 刪除舊角色
                        await self.rpg.db.execute(
                            "DELETE FROM rpg_players WHERE user_id = $1",
                            self.user_id
                        )
                        
                        # 創建新角色
                        success = await self.rpg.player.create_simple_player(
                            self.user_id, 
                            f"冒險者{self.user_id}"
                        )
                        
                        if success:
                            embed = discord.Embed(
                                title="✅ 角色已重置",
                                description="角色已成功刪除並重新創建",
                                color=0x00FF00
                            )
                            await interaction.followup.send(embed=embed, ephemeral=True)
                        else:
                            await interaction.followup.send("❌ 重置失敗", ephemeral=True)
                            
                    except Exception as e:
                        await interaction.followup.send(f"❌ 重置失敗: {e}", ephemeral=True)
            
            embed.add_field(
                name="🛠️ 修復選項",
                value="點擊下方按鈕執行修復",
                inline=False
            )
            
            await interaction.followup.send(embed=embed, view=FixView(self.rpg, user_id), ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ 修復工具錯誤: {e}", ephemeral=True)
    
    @app_commands.command(name="rpg_debug", description="RPG 系統診斷")
    async def rpg_debug_slash(self, interaction: discord.Interaction):
        """RPG 診斷"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            user_id = interaction.user.id
            
            embed = discord.Embed(
                title="🔧 RPG 系統診斷報告",
                color=0x7289DA
            )
            
            # 檢查資料庫連接
            if self.rpg.db.is_connected:
                embed.add_field(name="🔌 資料庫連接", value="✅ 正常", inline=True)
            else:
                embed.add_field(name="🔌 資料庫連接", value="❌ 未連接", inline=True)
            
            # 檢查角色
            player = await self.rpg.player.get_player_safe(user_id)
            if player:
                embed.add_field(
                    name="👤 你的角色狀態",
                    value=f"✅ 角色存在\n名稱: {player.get('nickname', '未知')}\n等級: {player.get('level', 1)}",
                    inline=True
                )
            else:
                embed.add_field(
                    name="👤 你的角色狀態",
                    value="❌ 角色不存在",
                    inline=True
                )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            await interaction.followup.send(f"❌ 診斷失敗: {e}", ephemeral=True)
    
    # ========== 其他 RPG 指令 (待實現) ==========
    
    @app_commands.command(name="rpg_inventory", description="查看背包")
    async def rpg_inventory_slash(self, interaction: discord.Interaction):
        """查看背包"""
        await interaction.response.defer(ephemeral=True)
        
        try:
            player = await self.rpg.player.get_player_safe(interaction.user.id)
            if not player:
                embed = discord.Embed(
                    title="❌ 角色不存在",
                    description="請先使用 `/rpg_start` 創建角色",
                    color=0xFF0000
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                return
            
            embed = discord.Embed(
                title="🎒 背包系統",
                description="背包功能開發中...",
                color=0x7289DA
            )
            
            embed.add_field(
                name="📦 背包資訊",
                value=(
                    f"**角色：** {player['nickname']}\n"
                    f"**負重：** {player['carrying_capacity']}\n"
                    f"**倉庫容量：** {player['storage_capacity']}"
                ),
                inline=False
            )
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ 讀取背包失敗",
                description=f"錯誤：{str(e)}",
                color=0xFF0000
            )
            await interaction.followup.send(embed=error_embed, ephemeral=True)
    
    @app_commands.command(name="rpg_help", description="顯示 RPG 幫助訊息")
    async def rpg_help_slash(self, interaction: discord.Interaction):
        """RPG 幫助"""
        embed = discord.Embed(
            title="🎮 RPG 系統幫助",
            description="阿爾比恩大陸冒險指南",
            color=0x7289DA
        )
        
        embed.add_field(
            name="🎯 核心指令",
            value=(
                "`/rpg_start` - 開始冒險\n"
                "`/rpg_status` - 角色狀態\n"
                "`/rpg_inventory` - 查看背包\n"
                "`/rpg_fix` - 修復角色問題\n"
                "`/rpg_debug` - 系統診斷\n"
                "`/rpg_help` - 顯示此訊息"
            ),
            inline=False
        )
        
        embed.add_field(
            name="⚔️ 即將推出",
            value=(
                "• 冒險探索系統\n"
                "• 戰鬥系統\n"
                "• 裝備系統\n"
                "• 商店系統\n"
                "• 組隊系統"
            ),
            inline=False
        )
        
        await interaction.response.send_message(embed=embed)

async def setup(bot):
    from rpg import RPGSYSTEM
    from database import db
    
    # 創建 RPG 系統實例
    rpg_system = RPGSYSTEM(db)
    await rpg_system.initialize()
    
    # 添加 RPG 指令
    await bot.add_cog(RPGCommands(bot, rpg_system))
