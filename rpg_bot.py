import discord
from discord.ext import commands, tasks
import asyncio

class RPGBot(commands.Bot):
    def __init__(self, command_prefix, intents):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.db = RPGDatabase()
        self.monster_system = MonsterSystem(self.db)
        self.item_system = ItemSystem(self.db)
        self.combat_system = CombatSystem(self.db)
        self.party_system = PartySystem(self.db)
        
    async def on_ready(self):
        print(f'Logged in as {self.user}')
        await self.change_presence(activity=discord.Game(name="小雲RPG"))
        
    async def setup_hook(self):
        # 添加指令
        self.tree.add_command(self.create_rpg_commands())
        
    def create_rpg_commands(self):
        """創建所有 RPG 指令"""
        @app_commands.command(name="rpg_start", description="開始 RPG 冒險")
        async def rpg_start(interaction: discord.Interaction, name: str):
            """創建角色指令"""
            user_id = str(interaction.user.id)
            
            player = self.db.get_player(user_id)
            if player:
                await interaction.response.send_message("⚠️ 你已經有角色了！", ephemeral=True)
                return
                
            self.db.create_player(user_id, name)
            await interaction.response.send_message(
                f"🎮 **歡迎來到小雲RPG！**\n"
                f"角色 **{name}** 創建成功！\n\n"
                f"📍 起始地點：孤兒院\n"
                f"💼 背包空間：20格\n"
                f"💰 起始資金：100金幣\n\n"
                f"使用 `/rpg_status` 查看狀態\n"
                f"使用 `/rpg_help` 查看指令"
            )
            
        @app_commands.command(name="rpg_status", description="查看角色狀態")
        async def rpg_status(interaction: discord.Interaction):
            """查看狀態指令"""
            user_id = str(interaction.user.id)
            player = self.db.get_player(user_id)
            
            if not player:
                await interaction.response.send_message(
                    "❌ 你還沒有角色！使用 `/rpg_start` 創建角色",
                    ephemeral=True
                )
                return
                
            embed = discord.Embed(
                title=f"{player['name']} 的狀態",
                color=discord.Color.blue()
            )
            
            embed.add_field(name="📊 基本資料", 
                          value=f"等級: {player['level']}\n"
                                f"經驗: {player['exp']}/{player['level']*100}\n"
                                f"位置: {player['location']}", 
                          inline=False)
            
            embed.add_field(name="❤️ 生命/魔力", 
                          value=f"HP: {player['hp']}/{player['max_hp']}\n"
                                f"MP: {player['mp']}/{player['max_mp']}", 
                          inline=True)
            
            embed.add_field(name="⚔️ 屬性", 
                          value=f"體力: {player['stamina']}\n"
                                f"速度: {player['speed']}\n"
                                f"力量: {player['strength']}\n"
                                f"智慧: {player['intelligence']}\n"
                                f"負重: {player['carry_capacity']}", 
                          inline=True)
            
            await interaction.response.send_message(embed=embed)
            
        @app_commands.command(name="party_create", description="創建隊伍")
        async def party_create(interaction: discord.Interaction):
            """創建隊伍指令"""
            user_id = str(interaction.user.id)
            channel_id = str(interaction.channel_id)
            
            party_id = self.party_system.create_party(user_id, channel_id)
            
            embed = discord.Embed(
                title="🏰 隊伍創建成功！",
                description="使用以下 Emoji 邀請其他人：\n\n"
                          "🟢 - 加入隊伍\n"
                          "🔴 - 離開隊伍\n"
                          "⚔️ - 開始冒險",
                color=discord.Color.green()
            )
            
            embed.add_field(name="隊伍狀態", 
                          value=self.party_system.get_party_status_emoji(party_id), 
                          inline=False)
            
            message = await interaction.response.send_message(embed=embed)
            
            # 添加反應按鈕
            msg = await interaction.original_response()
            await msg.add_reaction("🟢")
            await msg.add_reaction("🔴")
            await msg.add_reaction("⚔️")
            
        @app_commands.command(name="adventure", description="開始冒險")
        async def adventure(interaction: discord.Interaction, map_id: str = "forest"):
            """開始冒險指令"""
            user_id = str(interaction.user.id)
            
            # 檢查是否在隊伍中
            party_info = self.party_system.get_user_party(user_id)
            
            if not party_info:
                # 單人冒險
                await interaction.response.send_message(
                    "🌲 開始單人冒險！\n"
                    "⚠️ 注意：單人冒險較危險，建議組隊",
                    ephemeral=True
                )
                # 開始冒險邏輯...
            else:
                # 隊伍冒險
                party_id = party_info["party_id"]
                
                # 檢查隊伍是否準備好
                all_ready = self.party_system.check_party_ready(party_id)
                
                if all_ready:
                    embed = discord.Embed(
                        title="🚀 隊伍冒險開始！",
                        description="進入幽暗森林第1層...",
                        color=discord.Color.green()
                    )
                    
                    # 隨機遭遇怪物
                    monster = self.monster_system.get_monster_for_floor(map_id, 1)
                    
                    embed.add_field(name="🐾 遭遇怪物！", 
                                  value=f"**{monster['name']}** (Lv.{monster['level']})\n"
                                        f"❤️ HP: {monster['hp']}\n"
                                        f"⚔️ 攻擊: {monster['attack']}",
                                  inline=False)
                    
                    await interaction.response.send_message(embed=embed)
                    
                    # 添加行動按鈕
                    view = CombatView(party_id, monster)
                    await interaction.followup.send("選擇你的行動：", view=view, ephemeral=False)
                    
        return app_commands.Group(name="rpg", description="RPG遊戲指令")
