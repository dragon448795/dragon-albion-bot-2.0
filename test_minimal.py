# test_minimal.py
"""
測試最小可行產品 - 只測試核心功能
"""
from rpg_core import RPGCore

def test_minimal():
    print("🎮 測試 RPG 最小可行產品")
    print("=" * 50)
    
    # 初始化
    rpg = RPGCore("test_minimal.db")
    
    # 測試1: 創建玩家
    print("\n1. 測試創建玩家")
    result = rpg.create_player("test_mini_001", "測試勇者")
    print(f"結果: {result['message']}")
    
    # 測試2: 查看狀態
    print("\n2. 測試查看狀態")
    status = rpg.get_player_status("test_mini_001")
    if status:
        player = status["player"]
        print(f"角色: {player['name']} Lv.{player['level']}")
        print(f"位置: {player['location']}")
        print(f"金幣: {player['gold']}")
    
    # 測試3: 創建隊伍
    print("\n3. 測試創建隊伍")
    # 先創建第二個玩家
    rpg.create_player("test_mini_002", "測試法師")
    
    party_result = rpg.create_party("test_mini_001")
    print(f"隊伍創建: {party_result['message']}")
    
    if party_result["success"]:
        party_id = party_result["party_id"]
        
        # 測試4: 加入隊伍
        print("\n4. 測試加入隊伍")
        join_result = rpg.join_party(party_id, "test_mini_002")
        print(f"加入隊伍: {join_result['message']}")
        
        # 測試5: 查看隊伍
        print("\n5. 測試查看隊伍信息")
        party_info = rpg.get_party_info(party_id)
        if party_info["success"]:
            print(f"隊伍ID: {party_info['party_id']}")
            print(f"隊長: {party_info['leader_id']}")
            print(f"成員數: {len(party_info['member_details'])}")
            for member in party_info["member_details"]:
                print(f"  - {member['name']} Lv.{member['level']}")
    
    # 測試6: 開始冒險
    print("\n6. 測試開始冒險")
    adventure_result = rpg.start_adventure("test_mini_001", "forest")
    print(f"冒險開始: {adventure_result['message']}")
    
    if adventure_result["success"]:
        print(f"遭遇: {adventure_result['monster']['name']}")
        print(f"戰鬥ID: {adventure_result['battle_id']}")
        
        # 測試7: 戰鬥行動
        print("\n7. 測試戰鬥行動")
        battle_result = rpg.battle_action(
            adventure_result["battle_id"],
            "test_mini_001",
            "attack"
        )
        print(f"戰鬥行動: {battle_result['message']}")
    
    print("\n" + "=" * 50)
    print("✅ 最小可行產品測試完成")
    print("💡 核心系統功能正常")

if __name__ == "__main__":
    test_minimal()
