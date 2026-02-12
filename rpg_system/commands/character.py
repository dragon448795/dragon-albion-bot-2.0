"""
角色相關指令: /rpg start, /rpg profile, /rpg stats
"""
import discord
from discord import app_commands
import json

def setup_character_commands(group: app_commands.Group, rpg):
    
    @group.command(name="start", description="開始你的 RPG 冒險！")
    async def rpg_start(interaction: discord.Interaction):
        await interaction.response.defer()
        
        try:
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
            
            # === 創建新角色 ===
            print(f"📝 正在為 {interaction.user.name} 創建 RPG 角色...")
            
            # 1. 插入角色資料
            await rpg.db.execute('''
                INSERT INTO rpg_characters (
                    user_id, guild_id,
                    base_vit, base_str, base_agi, base_int, base_luck,
                    hp, max_hp, mp, max_mp,
                    current_house, storage_size, coins
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ''', 
                interaction.user.id, 
                interaction.guild.id,
                10, 10, 10, 10, 10,  # 基礎屬性
                100, 100,            # HP, max_hp
                50, 50,             # MP, max_mp
                '孤兒院',           # current_house
                20,                # storage_size
                100               # coins
            )
            
            # 2. 初始化空裝備欄
            await rpg.db.execute('''
                INSERT INTO rpg_equipment (
                    user_id, guild_id,
                    weapon, head, body, shoes, necklace, ring, backpack
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', 
                interaction.user.id, 
                interaction.guild.id,
                None, None, None, None, None, None, None
            )
            
            # 3. 初始化房屋
            await rpg.db.execute('''
                INSERT INTO rpg_houses (
                    user_id, guild_id, house_type, storage_items, crafting_facilities
                ) VALUES ($1, $2, $3, $4, $5)
            ''', 
                interaction.user.id, 
                interaction.guild.id,
                '孤兒院',
                json.dumps([]),
                json.dumps([])
            )
            
            print(f"✅ {interaction.user.name} 的 RPG 角色創建成功！")
            
            embed = discord.Embed(
                title="🎮 冒險者，歡迎來到小雲世界！",
                description=(
                    f"你來自 **小雲孤兒院**，現在是時候踏上冒險旅程了！\n\n"
                    f"**初始屬性：**\n"
                    f"❤️ 體力：10 (HP: 100)\n"
                    f"⚡ 速度：10 (閃避: 5%)\n"
                    f"💪 力量：10 (攻擊: 20)\n"
                    f"🧠 智慧：10 (MP: 50, 魔攻: 20)\n"
                    f"🎒 負重：10 (背包: 20格)\n\n"
                    f"使用 `/rpg profile` 查看詳細狀態"
                ),
                color=0x43B581
            )
            
            if interaction.user.avatar:
                embed.set_thumbnail(url=interaction.user.avatar.url)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            print(f"❌ 創建角色失敗: {e}")
            import traceback
            traceback.print_exc()
            
            embed = discord.Embed(
                title="❌ 創建角色失敗",
                description=f"錯誤: {str(e)[:100]}",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
    
    @group.command(name="profile", description="查看角色狀態")
    async def rpg_profile(interaction: discord.Interaction):
        await interaction.response.defer()
        
        try:
            # 獲取角色資料
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
            total_vit = char['base_vit'] + char['bonus_vit']
            total_str = char['base_str'] + char['bonus_str']
            total_agi = char['base_agi'] + char['bonus_agi']
            total_int = char['base_int'] + char['bonus_int']
            total_luck = char['base_luck'] + char['bonus_luck']
            
            total_hp = total_vit * 10
            total_mp = total_int * 5
            
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
                name="⚔️ 戰鬥屬性",
                value=(
                    f"**物理攻擊：** {total_str * 2}\n"
                    f"**物理防禦：** {total_str}\n"
                    f"**魔法攻擊：** {total_int * 2}\n"
                    f"**魔法防禦：** {total_int}\n"
                    f"**閃避率：** {total_agi * 0.5:.1f}%"
                ),
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
            
            if interaction.user.avatar:
                embed.set_thumbnail(url=interaction.user.avatar.url)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            print(f"❌ 讀取角色資料失敗: {e}")
            import traceback
            traceback.print_exc()
            
            embed = discord.Embed(
                title="❌ 讀取失敗",
                description=f"錯誤: {str(e)[:100]}",
                color=0xFF0000
            )
            await interaction.followup.send(embed=embed)
