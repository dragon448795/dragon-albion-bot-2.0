"""
RPG 系統主入口
"""
from .core import RPGSystem, get_rpg_system

# 只有當 commands 模組存在時才導入
try:
    from .commands import register_all_commands
    __all__ = ['RPGSystem', 'get_rpg_system', 'register_all_commands']
except ImportError:
    __all__ = ['RPGSystem', 'get_rpg_system']
    print("⚠️ RPG 系統：指令模組未載入")
