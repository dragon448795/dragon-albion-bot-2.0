"""
角色相關指令: /rpg start, /rpg profile, /rpg stats
"""
import discord
from discord import app_commands

def setup_character_commands(group: app_commands.Group, rpg):
    
    @group.command(name="start", description="開始你的 RPG 冒險！")
    async def rpg_start(interaction: discord.Interaction):
        await interaction.response.defer()
        
        # 檢查是否已有角色
        char = await rpg.db.fetchrow(
            "SELECT * FROM rpg_characters WHERE user_id = $1 AND guild_id = $2",
            interaction.user.id, interaction.guild.id
        )
        
        if char:
            embed = discord.Embed(
                title="⚠️ 角色已存在",
                description="你已經有冒險者角色了！",
                color=0xFFA500
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 創建新角色
        await rpg.db.execute('''
            INSERT INTO rpg_characters (user_id, guild_id, username)
            VALUES ($1, $2, $3)
        ''', interaction.user.id, interaction.guild.id, interaction.user.name)
        
        # 初始化空裝備欄
        await rpg.db.execute('''
            INSERT INTO rpg_equipment (user_id, guild_id)
            VALUES ($1, $2)
        ''', interaction.user.id, interaction.guild.id)
        
        # 初始化房屋
        await rpg.db.execute('''
            INSERT INTO rpg_houses (user_id, guild_id)
            VALUES ($1, $2)
        ''', interaction.user.id, interaction.guild.id)
        
        embed = discord.Embed(
            title="🎮 冒險者，歡迎來到小雲世界！",
            description=(
                f"你來自 **小雲孤兒院**，現在是時候踏上冒險旅程了！\n\n"
                f"**初始屬性：**\n"
                f"❤️ 體力：10\n"
                f"⚡ 速度：10\n"
                f"💪 力量：10\n"
                f"🧠 智慧：10\n"
                f"🎒 負重：10\n\n"
                f"使用 `/rpg profile` 查看詳細狀態"
            ),
            color=0x43B581
        )
        
        await interaction.followup.send(embed=embed)
    
    @group.command(name="profile", description="查看角色狀態")
    async def rpg_profile(interaction: discord.Interaction):
        await interaction.response.defer()
        
        char = await rpg.db.fetchrow(
            "SELECT * FROM rpg_characters WHERE user_id = $1 AND guild_id = $2",
            interaction.user.id, interaction.guild.id
        )
        
        if not char:
            embed = discord.Embed(
                title="❌ 角色不存在",
                description="請先使用 `/rpg start` 創建角色",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
            return
        
        # 計算總屬性
        total_hp = char['base_vit'] * 10 + char['bonus_vit'] * 10
        total_mp = char['base_int'] * 5 + char['bonus_int'] * 5
        
        embed = discord.Embed(
            title=f"⚔️ {interaction.user.name} 的冒險者檔案",
            color=0x7289DA
        )
        
        embed.add_field(
            name="📊 基本資訊",
            value=(
                f"**等級：** {char['level']} ({char['exp']}/{char['exp_next']} EXP)\n"
                f"**房屋：** {char['current_house']}\n"
                f"**金幣：** {char['coins']:,}\n"
                f"**未分配點數：** {char['unspent_stats']}"
            ),
            inline=False
        )
        
        embed.add_field(
            name="❤️ 生命狀態",
            value=f"HP: {char['hp']}/{total_hp}\nMP: {char['mp']}/{total_mp}",
            inline=True
        )
        
        embed.add_field(
            name="📈 基礎屬性",
            value=(
                f"**體力：** {char['base_vit']} (+{char['bonus_vit']})\n"
                f"**力量：** {char['base_str']} (+{char['bonus_str']})\n"
                f"**速度：** {char['base_agi']} (+{char['bonus_agi']})\n"
                f"**智慧：** {char['base_int']} (+{char['bonus_int']})\n"
                f"**負重：** {char['base_luck']} (+{char['bonus_luck']})"
            ),
            inline=True
        )
        
        await interaction.followup.send(embed=embed)
