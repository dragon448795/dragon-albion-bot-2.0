"""
隊伍相關指令: /rpg party create, /rpg party join, /rpg party leave
"""
import discord
from discord import app_commands
import json

def setup_party_commands(group: app_commands.Group, rpg):
    
    @group.command(name="party_create", description="創建隊伍")
    async def rpg_party_create(interaction: discord.Interaction):
        await interaction.response.defer()
        
        # 檢查是否已經在隊伍中
        existing = await rpg.db.fetchrow(
            "SELECT * FROM rpg_parties WHERE members::jsonb ? $1 AND is_active = true AND guild_id = $2",
            str(interaction.user.id), interaction.guild.id
        )
        
        if existing:
            await interaction.followup.send("❌ 你已經在一個隊伍中了")
            return
        
        party_id = await rpg.db.fetchval('''
            INSERT INTO rpg_parties (leader_id, members, guild_id)
            VALUES ($1, $2, $3)
            RETURNING party_id
        ''', interaction.user.id, json.dumps([interaction.user.id]), interaction.guild.id)
        
        embed = discord.Embed(
            title="✅ 隊伍創建成功",
            description=f"隊伍ID: `{party_id}`\n使用 `/rpg party join {party_id}` 加入隊伍",
            color=0x00FF00
        )
        
        await interaction.followup.send(embed=embed)
    
    @group.command(name="party_join", description="加入隊伍")
    @app_commands.describe(party_id="隊伍ID")
    async def rpg_party_join(interaction: discord.Interaction, party_id: int):
        await interaction.response.defer()
        
        party = await rpg.db.fetchrow(
            "SELECT * FROM rpg_parties WHERE party_id = $1 AND is_active = true AND guild_id = $2",
            party_id, interaction.guild.id
        )
        
        if not party:
            await interaction.followup.send("❌ 隊伍不存在")
            return
        
        members = party['members']
        if isinstance(members, str):
            members = json.loads(members)
        
        if len(members) >= 5:
            await interaction.followup.send("❌ 隊伍已滿（上限5人）")
            return
        
        if interaction.user.id in members:
            await interaction.followup.send("❌ 你已經在隊伍中了")
            return
        
        members.append(interaction.user.id)
        
        await rpg.db.execute(
            "UPDATE rpg_parties SET members = $1 WHERE party_id = $2",
            json.dumps(members), party_id
        )
        
        await interaction.followup.send(f"✅ 成功加入隊伍！目前成員: {len(members)}/5")
