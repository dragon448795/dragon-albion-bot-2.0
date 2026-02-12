"""
房屋相關指令: /rpg house, /rpg house upgrade
"""
import discord
from discord import app_commands

HOUSE_TYPES = {
    "孤兒院": {"level": 0, "storage": 20, "price": 0, "emoji": "🏚️"},
    "小房屋": {"level": 1, "storage": 50, "price": 5000, "emoji": "🏠"},
    "中房屋": {"level": 2, "storage": 75, "price": 15000, "emoji": "🏡"},
    "大房屋": {"level": 3, "storage": 100, "price": 50000, "emoji": "🏘️"},
    "領地": {"level": 4, "storage": 400, "price": 200000, "emoji": "🏰"},
    "城堡": {"level": 5, "storage": 1000, "price": 1000000, "emoji": "🏯"}
}

def setup_house_commands(group: app_commands.Group, rpg):
    
    @group.command(name="house", description="查看房屋狀態")
    async def rpg_house(interaction: discord.Interaction):
        await interaction.response.defer()
        
        house = await rpg.db.fetchrow(
            "SELECT * FROM rpg_houses WHERE user_id = $1 AND guild_id = $2",
            interaction.user.id, interaction.guild.id
        )
        
        if not house:
            await interaction.followup.send("❌ 房屋資料不存在")
            return
        
        house_type = house['house_type']
        house_info = HOUSE_TYPES.get(house_type, HOUSE_TYPES["孤兒院"])
        
        embed = discord.Embed(
            title=f"{house_info['emoji']} {house_type}",
            color=0x2ECC71
        )
        
        embed.add_field(name="📦 儲存空間", value=f"{house_info['storage']} 格", inline=True)
        embed.add_field(name="💰 升級價格", value=f"{house_info['price']:,} 金幣", inline=True)
        
        await interaction.followup.send(embed=embed)
    
    @group.command(name="house_upgrade", description="升級房屋")
    async def rpg_house_upgrade(interaction: discord.Interaction):
        await interaction.response.defer()
        
        house = await rpg.db.fetchrow(
            "SELECT * FROM rpg_houses WHERE user_id = $1 AND guild_id = $2",
            interaction.user.id, interaction.guild.id
        )
        
        if not house:
            await interaction.followup.send("❌ 房屋資料不存在")
            return
        
        current_type = house['house_type']
        house_types = list(HOUSE_TYPES.keys())
        current_index = house_types.index(current_type)
        
        if current_index >= len(house_types) - 1:
            await interaction.followup.send("🏆 已經是最頂級房屋了！")
            return
        
        next_type = house_types[current_index + 1]
        next_info = HOUSE_TYPES[next_type]
        
        # 檢查金幣（這裡需要實作金幣檢查）
        
        await rpg.db.execute(
            "UPDATE rpg_houses SET house_type = $1 WHERE user_id = $2 AND guild_id = $3",
            next_type, interaction.user.id, interaction.guild.id
        )
        
        embed = discord.Embed(
            title="🏠 房屋升級成功！",
            description=f"{house_info['emoji']} {current_type} → {next_info['emoji']} {next_type}",
            color=0x00FF00
        )
        
        await interaction.followup.send(embed=embed)
