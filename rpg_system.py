# ========== RPG 系統整合（完全獨立，不影響原有功能）==========
try:
    print("🔄 正在載入 RPG 系統模組...")
    from rpg_system import RPGSystem, get_rpg_system
    
    # 建立全域 RPG 實例
    rpg = get_rpg_system(bot, db, memory_cache)
    print("✅ RPG 系統實例已創建")
    
    # 儲存原本的 on_ready
    original_on_ready = bot.on_ready
    
    @bot.event
    async def on_ready():
        # 先執行原本的 on_ready
        if original_on_ready:
            await original_on_ready()
        
        print("🎮 正在初始化 RPG 系統...")
        
        # 延遲初始化，確保 Bot 完全就緒
        await asyncio.sleep(2)
        
        # 初始化 RPG 資料庫
        await rpg.initialize()
        
        # 註冊 RPG 指令
        await rpg.register_commands(bot.tree)
        
        # 同步指令
        try:
            synced = await bot.tree.sync()
            print(f"✅ 全域指令同步完成，共 {len(synced)} 個指令")
            
            # 確認 RPG 指令是否存在
            cmd_names = [cmd.name for cmd in synced]
            if 'rpg' in cmd_names:
                print("✅ RPG 指令群組已成功同步！")
            else:
                print("❌ RPG 指令群組同步失敗！")
                
        except Exception as e:
            print(f"❌ 指令同步失敗: {e}")
    
    print("🔌 RPG 系統載入點已準備")
    
except Exception as e:
    print(f"⚠️ 未檢測到 RPG 系統模組: {e}")
    rpg = None

# ========== 啟動機器人 ==========
if __name__ == "__main__":
    main()
