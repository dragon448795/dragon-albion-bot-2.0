# ========== 主程式 ==========
def main():
    """主程式入口"""
    print(f"{'='*50}")
    print(f"🚀 啟動 {BOT_NAME} - PostgreSQL 完整修正版本")
    print(f"💡 完整功能：17個指令 + 聊天積分 + 評核活動 + 抽獎系統")
    print(f"🔧 擁有者ID: {OWNER_IDS}")
    print(f"🗄️ 資料庫: PostgreSQL (Railway) + 記憶體緩存備份")
    print(f"📊 指令數量: 17個 (11用戶 + 4管理員 + 2系統)")
    print(f"{'='*50}")
    
    token = os.getenv("DISCORD_TOKEN")
    
    if not token or token == "你的_bot_token_在這裡":
        print("❌ 找不到有效的 Token！")
        print("💡 請在 Railway 設定環境變數：")
        print("   1. 進入 Railway 專案")
        print("   2. 點擊 Settings")
        print("   3. 點擊 Variables")
        print("   4. 新增 DISCORD_TOKEN = 你的_bot_token")
        sys.exit(1)
    
    print("✅ Token 讀取成功")
    print("🔄 正在連接 Discord...")
    
    try:
        bot.run(token)
    except discord.LoginFailure:
        print("❌ 登入失敗！請檢查 Token 是否正確")
        print("💡 請到 Discord Developer Portal 重置 Token")
    except Exception as e:
        print(f"❌ 啟動失敗: {e}")
        traceback.print_exc()

# ========== RPG 系統整合（完全獨立，不影響原有功能）==========
# 注意：這部分必須放在 main() 之前，但不能在 if __name__ == "__main__": 區塊內

try:
    from rpg_system import RPGSystem, get_rpg_system
    rpg = get_rpg_system(bot, db, memory_cache)
    
    # 儲存原本的 on_ready
    original_on_ready = bot.on_ready
    
    @bot.event
    async def on_ready():
        # 先執行原本的 on_ready
        if original_on_ready:
            await original_on_ready()
        
        # ========== 延遲初始化，確保 Bot 完全就緒 ==========
        await asyncio.sleep(2)
        
        # 初始化 RPG 資料庫
        await rpg.initialize()
        
        # 註冊 RPG 指令
        await rpg.register_commands(bot.tree)
        
        # ========== 同步所有指令（原有 + RPG）==========
        try:
            synced = await bot.tree.sync()
            print(f"✅ 全域指令同步完成，共 {len(synced)} 個指令")
            
            # 列出所有指令名稱
            cmd_names = [cmd.name for cmd in synced]
            print(f"📋 已同步指令: {', '.join(cmd_names)}")
            
            # 確認 RPG 指令是否存在
            if 'rpg' in cmd_names:
                print("✅ RPG 指令群組已成功同步！")
            else:
                print("❌ RPG 指令群組同步失敗！")
                
        except Exception as e:
            print(f"❌ 指令同步失敗: {e}")
    
    print("🔌 RPG 系統載入點已準備")
    
except ImportError as e:
    print(f"ℹ️ 未檢測到 RPG 系統模組: {e}")
    print("   如需使用 RPG 功能，請建立 rpg_system.py")
    rpg = None

if __name__ == "__main__":
    main()
