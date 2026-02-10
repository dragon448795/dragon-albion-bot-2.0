# rpg/player.py
import discord
from datetime import datetime
from config import RPG_CONFIG
from utils import create_progress_bar

class RPGPlayerSystem:
    def __init__(self, db):
        self.db = db
    
    async def get_player_safe(self, user_id: int):
        """安全獲取 RPG 玩家資料"""
        if not self.db.is_connected:
            return None
        
        try:
            player = await self.db.fetchrow(
                '''
                SELECT * FROM rpg_players WHERE user_id = $1
                ''',
                user_id
            )
            return player
        except Exception as e:
            print(f"❌ 獲取 RPG 玩家資料失敗: {e}")
            return None
    
    async def create_simple_player(self, user_id: int, nickname: str = None) -> bool:
        """簡化版角色創建"""
        if not self.db.is_connected:
            return False
        
        try:
            username = nickname or f"冒險者{user_id}"
            
            # 檢查是否已存在
            existing = await self.db.fetchval(
                "SELECT 1 FROM rpg_players WHERE user_id = $1",
                user_id
            )
            
            if existing:
                return True
            
            # 創建新角色（只包含必要欄位）
            await self.db.execute('''
                INSERT INTO rpg_players (
                    user_id, nickname, level, exp, max_exp,
                    vitality, speed, strength, intelligence, carrying_capacity,
                    current_hp, max_hp, current_mp, max_mp,
                    house_type, storage_capacity,
                    current_map, current_layer, is_in_town,
                    created_at, last_active
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            ''',
                user_id,
                username,
                1,      # level
                0,      # exp
                100,    # max_exp
                10,     # vitality
                10,     # speed
                10,     # strength
                10,     # intelligence
                10,     # carrying_capacity
                100,    # current_hp
                100,    # max_hp
                50,     # current_mp
                50,     # max_mp
                'orphanage',    # house_type
                20,             # storage_capacity
                '新手森林',     # current_map
                1,              # current_layer
                True,           # is_in_town
                datetime.now(), # created_at
                datetime.now()  # last_active
            )
            
            print(f"✅ RPG 角色創建成功: {user_id} - {username}")
            return True
            
        except Exception as e:
            print(f"❌ RPG 角色創建失敗: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def create_status_embed(self, user, player):
        """創建角色狀態 Embed"""
        embed = discord.Embed(
            title=f"📊 {user.display_name} 的 RPG 狀態",
            color=0x7289DA
        )
        
        # HP/MP 狀態條
        hp_percent = (player['current_hp'] / player['max_hp']) * 100
        mp_percent = (player['current_mp'] / player['max_mp']) * 100
        
        hp_bar = create_progress_bar(hp_percent, 15)
        mp_bar = create_progress_bar(mp_percent, 15)
        
        embed.add_field(
            name="❤️‍🩹 狀態",
            value=(
                f"**HP：** {hp_bar}\n"
                f"`{player['current_hp']}/{player['max_hp']}`\n\n"
                f"**MP：** {mp_bar}\n"
                f"`{player['current_mp']}/{player['max_mp']}`"
            ),
            inline=True
        )
        
        # 基本資訊
        embed.add_field(
            name="👤 基本資訊",
            value=(
                f"**角色：** {player['nickname']}\n"
                f"**等級：** {player['level']} 📊\n"
                f"**經驗：** {player['exp']}/{player['max_exp']} ⭐\n"
                f"**位置：** {player['current_map']}"
            ),
            inline=True
        )
        
        # 屬性
        embed.add_field(
            name="📈 屬性",
            value=(
                f"**體力：** {player['vitality']} ❤️\n"
                f"**速度：** {player['speed']} ⚡\n"
                f"**力量：** {player['strength']} 💪\n"
                f"**智慧：** {player['intelligence']} 🧠\n"
                f"**負重：** {player['carrying_capacity']} 🎒"
            ),
            inline=True
        )
        
        embed.set_footer(text=f"最後活動: {player['last_active'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        if user.avatar:
            embed.set_thumbnail(url=user.avatar.url)
        
        return embed
