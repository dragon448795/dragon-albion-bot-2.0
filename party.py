class PartySystem:
    def __init__(self, db):
        self.db = db
        
    def create_party(self, leader_id: str, channel_id: str):
        """創建隊伍"""
        party_id = f"party_{leader_id}_{int(datetime.now().timestamp())}"
        
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO parties (party_id, leader_id, channel_id) 
        VALUES (?, ?, ?)
        ''', (party_id, leader_id, channel_id))
        
        cursor.execute('''
        INSERT INTO party_members (party_id, user_id, position) 
        VALUES (?, ?, ?)
        ''', (party_id, leader_id, 1))
        
        conn.commit()
        conn.close()
        
        return party_id
    
    def join_party(self, party_id: str, user_id: str):
        """加入隊伍"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # 檢查隊伍人數
        cursor.execute('SELECT COUNT(*) FROM party_members WHERE party_id = ?', (party_id,))
        count = cursor.fetchone()[0]
        
        if count >= 5:
            conn.close()
            return False, "隊伍已滿（最多5人）"
            
        # 檢查是否已在隊伍中
        cursor.execute('SELECT * FROM party_members WHERE party_id = ? AND user_id = ?', 
                      (party_id, user_id))
        if cursor.fetchone():
            conn.close()
            return False, "已在隊伍中"
            
        cursor.execute('''
        INSERT INTO party_members (party_id, user_id, position) 
        VALUES (?, ?, ?)
        ''', (party_id, user_id, count + 1))
        
        conn.commit()
        conn.close()
        
        return True, "成功加入隊伍"
    
    def get_party_status_emoji(self, party_id: str):
        """取得隊伍狀態（Emoji顯示）"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT p.user_id, p.name, pm.position 
        FROM party_members pm
        JOIN players p ON pm.user_id = p.user_id
        WHERE pm.party_id = ?
        ORDER BY pm.position
        ''', (party_id,))
        
        members = cursor.fetchall()
        conn.close()
        
        # 使用 Emoji 顯示隊伍
        emoji_map = {1: "👑", 2: "⚔️", 3: "🛡️", 4: "🏹", 5: "🔮"}
        
        status_lines = []
        for member in members:
            user_id, name, position = member
            emoji = emoji_map.get(position, "👤")
            status_lines.append(f"{emoji} {name}")
            
        return "\n".join(status_lines)
