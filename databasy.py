import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import random

class RPGDatabase:
    def __init__(self, db_path="rpg_game.db"):
        self.db_path = db_path
        self.init_database()
        
    def init_database(self):
        """初始化所有資料表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 玩家資料表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS players (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            level INTEGER DEFAULT 1,
            exp INTEGER DEFAULT 0,
            hp INTEGER DEFAULT 100,
            max_hp INTEGER DEFAULT 100,
            mp INTEGER DEFAULT 50,
            max_mp INTEGER DEFAULT 50,
            stamina INTEGER DEFAULT 10,
            speed INTEGER DEFAULT 10,
            strength INTEGER DEFAULT 10,
            intelligence INTEGER DEFAULT 10,
            carry_capacity INTEGER DEFAULT 10,
            gold INTEGER DEFAULT 100,
            location TEXT DEFAULT '起始村莊',
            home_type TEXT DEFAULT '孤兒院',
            storage_capacity INTEGER DEFAULT 20,
            deaths INTEGER DEFAULT 0,
            total_play_time INTEGER DEFAULT 0,
            last_heal_time TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 裝備欄位表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS equipment (
            player_id TEXT,
            slot TEXT,
            item_id TEXT,
            FOREIGN KEY (player_id) REFERENCES players (user_id)
        )
        ''')
        
        # 背包物品表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT,
            item_id TEXT,
            quantity INTEGER,
            durability INTEGER,
            FOREIGN KEY (player_id) REFERENCES players (user_id)
        )
        ''')
        
        # 怪物資料表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS monsters (
            id TEXT PRIMARY KEY,
            name TEXT,
            level INTEGER,
            color TEXT,
            hp INTEGER,
            attack INTEGER,
            defense INTEGER,
            speed INTEGER,
            exp_reward INTEGER,
            gold_reward INTEGER,
            map_id TEXT,
            floor INTEGER,
            spawn_weight INTEGER
        )
        ''')
        
        # 物品資料表（素材、裝備、藥水等）
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS items (
            id TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,
            subtype TEXT,
            rarity TEXT,
            level INTEGER,
            value INTEGER,
            hp_bonus INTEGER DEFAULT 0,
            mp_bonus INTEGER DEFAULT 0,
            stamina_bonus INTEGER DEFAULT 0,
            speed_bonus INTEGER DEFAULT 0,
            strength_bonus INTEGER DEFAULT 0,
            intelligence_bonus INTEGER DEFAULT 0,
            capacity_bonus INTEGER DEFAULT 0,
            attack_bonus INTEGER DEFAULT 0,
            defense_bonus INTEGER DEFAULT 0,
            critical_chance_bonus REAL DEFAULT 0,
            critical_damage_bonus REAL DEFAULT 0,
            durability INTEGER DEFAULT 100,
            max_durability INTEGER DEFAULT 100,
            skill_name TEXT,
            skill_mp_cost INTEGER,
            weight INTEGER
        )
        ''')
        
        # 掉落表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS loot_table (
            monster_id TEXT,
            item_id TEXT,
            min_quantity INTEGER DEFAULT 1,
            max_quantity INTEGER DEFAULT 1,
            drop_chance REAL,
            FOREIGN KEY (monster_id) REFERENCES monsters (id),
            FOREIGN KEY (item_id) REFERENCES items (id)
        )
        ''')
        
        # 隊伍表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS parties (
            party_id TEXT PRIMARY KEY,
            leader_id TEXT,
            channel_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            map_id TEXT DEFAULT 'map_1',
            floor INTEGER DEFAULT 1,
            is_active INTEGER DEFAULT 1
        )
        ''')
        
        # 隊伍成員表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS party_members (
            party_id TEXT,
            user_id TEXT,
            position INTEGER,
            is_ready INTEGER DEFAULT 0,
            FOREIGN KEY (party_id) REFERENCES parties (party_id),
            FOREIGN KEY (user_id) REFERENCES players (user_id)
        )
        ''')
        
        # 拍賣行表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS auction_house (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seller_id TEXT,
            item_id TEXT,
            quantity INTEGER,
            price INTEGER,
            listed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            is_sold INTEGER DEFAULT 0
        )
        ''')
        
        conn.commit()
        conn.close()
        
    def get_player(self, user_id: str):
        """取得玩家資料"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM players WHERE user_id = ?', (user_id,))
        player = cursor.fetchone()
        
        conn.close()
        return dict(player) if player else None
    
    def create_player(self, user_id: str, name: str):
        """創建新玩家"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO players (user_id, name) VALUES (?, ?)
        ''', (user_id, name))
        
        conn.commit()
        conn.close()
        return True
