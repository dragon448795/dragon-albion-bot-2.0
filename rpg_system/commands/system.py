"""
系統相關指令: /rpg version, /rpg status
"""
import discord
from discord import app_commands

def setup_system_commands(group: app_commands.Group, rpg):
    
    @group.command(name="version", description="查看 RPG 系統版本")
    async def rpg_version(interaction: discord.Interaction):
        await interaction.response.send_message(
            "🎮 RPG 系統 v0.1 - 模組化版本",
            ephemeral=True
        )
    
    @group.command(name="status", description="檢查 RPG 資料庫狀態")
    async def rpg_status(interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        if not rpg.db.is_connected:
            await interaction.followup.send("❌ RPG 系統：資料庫未連接")
            return
        
        try:
            tables = await rpg.db.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'rpg_%'
            """)
            
            embed = discord.Embed(
                title="🎮 RPG 系統狀態",
                color=0x00FF00
            )
            embed.add_field(name="📊 RPG 表格數量", value=f"{len(tables)} 個", inline=True)
            embed.add_field(name="🔌 資料庫連接", value="✅ 正常", inline=True)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ 檢查失敗: {e}")
