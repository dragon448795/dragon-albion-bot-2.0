"""
RPG 指令註冊中心
"""
from discord import app_commands

async def register_all_commands(tree, rpg):
    """
    註冊所有 RPG 指令
    """
    print("🔄 正在註冊 RPG 指令...")
    
    # 建立主群組
    rpg_group = app_commands.Group(
        name="rpg",
        description="🎮 RPG 冒險系統"
    )
    
    # 延遲導入，避免循環依賴
    from .system import setup_system_commands
    from .character import setup_character_commands
    from .inventory import setup_inventory_commands
    from .house import setup_house_commands
    from .party import setup_party_commands
    
    # 註冊各個子系統的指令
    setup_system_commands(rpg_group, rpg)
    setup_character_commands(rpg_group, rpg)
    setup_inventory_commands(rpg_group, rpg)
    setup_house_commands(rpg_group, rpg)
    setup_party_commands(rpg_group, rpg)
    
    # 加到指令樹
    tree.add_command(rpg_group)
    print("✅ RPG 系統：所有指令註冊完成")
    return True
