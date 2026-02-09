import discord
from discord.ui import View, Button, Select

class CombatView(View):
    def __init__(self, party_id, monster):
        super().__init__(timeout=120)  # 2分鐘超時
        self.party_id = party_id
        self.monster = monster
        
    @discord.ui.button(label="⚔️ 攻擊", style=discord.ButtonStyle.primary, emoji="⚔️")
    async def attack_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        # 執行攻擊邏輯...
        
    @discord.ui.button(label="🛡️ 防禦", style=discord.ButtonStyle.secondary, emoji="🛡️")
    async def defense_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.defer()
        # 執行防禦邏輯...
        
    @discord.ui.button(label="❤️ 使用藥水", style=discord.ButtonStyle.success, emoji="❤️")
    async def potion_button(self, interaction: discord.Interaction, button: Button):
        # 打開藥水選擇選單
        await interaction.response.send_message("選擇藥水：", 
                                              view=PotionSelectView(self.party_id),
                                              ephemeral=True)
        
    @discord.ui.button(label="🏃 逃跑", style=discord.ButtonStyle.danger, emoji="🏃")
    async def flee_button(self, interaction: discord.Interaction, button: Button):
        # 逃跑邏輯
        await interaction.response.send_message("嘗試逃跑...", ephemeral=True)
