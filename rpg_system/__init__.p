"""
RPG 系統主入口
"""
from .core import RPGSystem, get_rpg_system
from .commands import register_all_commands

__all__ = ['RPGSystem', 'get_rpg_system', 'register_all_commands']
