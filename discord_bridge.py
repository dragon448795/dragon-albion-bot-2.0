# discord_bridge.py
"""
Discord Bot 橋接器 - 將 RPG 系統連接到 Discord
"""
from rpg_core import RPGCore
import discord

class DiscordRPGBridge:
    def __init__(self):
        self.rpg = RPGCore("discord_rpg.db")
        self.command_handlers = {
            "!rpg start": self.handle_start,
            "!rpg status": self.handle_status,
            "!rpg party": self.handle_party,
            "!rpg adventure": self.handle_adventure,
            "!rpg help": self.handle_help,
        }
    
    async def handle_message(self, message: discord.Message) -> str:
        """處理 Discord 消息"""
        content = message.content.strip()
        user_id = str(message.author.id)
        
        # 檢查是否是 RPG 指令
        for command, handler in self.command_handlers.items():
            if content.startswith(command):
                return await handler(user_id, content, message)
        
        return None  # 不是 RPG 指令
    
    async def handle_start(self, user_id: str, content: str, message: discord.Message) -> str:
        """處理創建角色指令"""
        parts = content.split()
        if len(parts) < 3:
            return "用法: `!rpg start [角色名稱]`"
        
        name = parts[2]
        result = self.rpg.create_player(user_id, name)
        
        if result["success"]:
            return (
                f"🎮 **角色創建成功！**\n\n"
                f"👤 歡迎 {name} 來到小雲 RPG！\n"
                f"📍 起始地點：孤兒院\n"
                f"💰 起始資金：100金幣\n"
                f"🎒 背包空間：20格\n\n"
                f"使用 `!rpg help` 查看所有指令"
            )
        else:
            return f"❌ {result['message']}"
    
    async def handle_status(self, user_id: str, content: str, message: discord.Message) -> str:
        """處理狀態指令"""
        status = self.rpg.get_player_status(user_id)
        
        if not status:
            return "❌ 你還沒有角色！使用 `!rpg start [名字]` 創建角色"
        
        player = status["player"]
        
        # 創建 Discord Embed
        embed = discord.Embed(
            title=f"{player['name']} 的狀態",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="📊 基本資料",
            value=f"等級: {player['level']}\n經驗: {player['exp']}/{player['level']*100}\n位置: {player['location']}",
            inline=False
        )
        
        embed.add_field(
            name="❤️ 生命/魔力",
            value=f"HP: {player['hp']}/{player['max_hp']}\nMP: {player['mp']}/{player['max_mp']}",
            inline=True
        )
        
        embed.add_field(
            name="💰 資源",
            value=f"金幣: {player['gold']}\n房屋: {player['home_type']}",
            inline=True
        )
        
        return embed
    
    async def handle_party(self, user_id: str, content: str, message: discord.Message) -> str:
        """處理隊伍指令"""
        parts = content.split()
        
        if len(parts) < 3:
            return (
                "👥 **隊伍指令**\n"
                "`!rpg party create` - 創建隊伍\n"
                "`!rpg party join [ID]` - 加入隊伍\n"
                "`!rpg party info` - 查看隊伍信息\n"
                "`!rpg party leave` - 離開隊伍"
            )
        
        subcommand = parts[2]
        
        if subcommand == "create":
            result = self.rpg.create_party(user_id)
            if result["success"]:
                return (
                    f"🏰 **隊伍創建成功！**\n\n"
                    f"隊伍ID: `{result['party_id']}`\n"
                    f"邀請碼: `{result['invite_code']}`\n\n"
                    f"分享邀請碼給朋友加入隊伍！"
                )
        
        elif subcommand == "join" and len(parts) > 3:
            party_code = parts[3]
            # 這裡需要根據邀請碼找到隊伍ID
            result = self.rpg.join_party(party_code, user_id)
            return result["message"]
        
        elif subcommand == "info":
            # 獲取玩家所在的隊伍
            status = self.rpg.get_player_status(user_id)
            if status and status.get("party"):
                party_info = self.rpg.get_party_info(status["party"])
                if party_info["success"]:
                    members_text = "\n".join([f"• {m['name']} Lv.{m['level']}" for m in party_info['member_details']])
                    return f"👥 **隊伍信息**\n\n成員:\n{members_text}"
            
            return "❌ 你不在任何隊伍中"
        
        return "❌ 未知的隊伍指令"
    
    async def handle_adventure(self, user_id: str, content: str, message: discord.Message) -> str:
        """處理冒險指令"""
        result = self.rpg.start_adventure(user_id)
        
        if result["success"]:
            monster = result["monster"]
            
            # 創建戰鬥界面
            embed = discord.Embed(
                title="🌲 開始冒險！",
                description=f"遭遇 **{monster['name']}**！",
                color=discord.Color.red()
            )
            
            embed.add_field(
                name="🐾 怪物信息",
                value=f"等級: {monster['level']}\nHP: {monster['hp']}\n攻擊: {monster['attack']}",
                inline=False
            )
            
            embed.add_field(
                name="⚔️ 行動指令",
                value="`!attack` - 攻擊\n`!defend` - 防禦\n`!flee` - 逃跑\n`!item` - 使用物品",
                inline=False
            )
            
            embed.set_footer(text=f"戰鬥ID: {result['battle_id']}")
            
            return embed
        else:
            return f"❌ {result['message']}"
    
    async def handle_help(self, user_id: str, content: str, message: discord.Message) -> str:
        """處理幫助指令"""
        embed = discord.Embed(
            title="🎮 小雲 RPG 幫助",
            description="歡迎來到小雲 RPG 世界！",
            color=discord.Color.green()
        )
        
        embed.add_field(
            name="🎯 基本指令",
            value=(
                "`!rpg start [名字]` - 創建角色\n"
                "`!rpg status` - 查看狀態\n"
                "`!rpg inventory` - 查看背包\n"
                "`!rpg shop` - 訪問商店"
            ),
            inline=False
        )
        
        embed.add_field(
            name="👥 隊伍指令",
            value=(
                "`!rpg party create` - 創建隊伍\n"
                "`!rpg party join [ID]` - 加入隊伍\n"
                "`!rpg party info` - 隊伍信息\n"
                "`!rpg party leave` - 離開隊伍"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🗺️ 冒險指令",
            value=(
                "`!rpg adventure` - 開始冒險\n"
                "`!rpg map` - 查看地圖\n"
                "`!rpg travel [地點]` - 前往地點"
            ),
            inline=False
        )
        
        embed.set_footer(text="更多功能開發中...")
        
        return embed
