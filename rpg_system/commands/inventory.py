"""
背包相關指令: /rpg inventory, /rpg use, /rpg equip
"""
import discord
from discord import app_commands
import json

def setup_inventory_commands(group: app_commands.Group, rpg):
    
    @group.command(name="inventory", description="查看背包")
    async def rpg_inventory(interaction: discord.Interaction):
        await interaction.response.defer()
        
        items = await rpg.db.fetch(
            "SELECT * FROM rpg_inventory WHERE user_id = $1 AND guild_id = $2 ORDER BY item_type, rarity DESC",
            interaction.user.id, interaction.guild.id
        )
        
        embed = discord.Embed(
            title=f"🎒 {interaction.user.name} 的背包",
            color=0xF1C40F
        )
        
        if not items:
            embed.description = "背包是空的..."
        else:
            materials = [i for i in items if i['item_type'] == 'material']
            consumables = [i for i in items if i['item_type'] == 'consumable']
            equipment = [i for i in items if i['item_type'] == 'equipment']
            
            if materials:
                mat_text = ""
                for item in materials[:10]:
                    mat_text += f"📦 **{item['item_name']}** x{item['quantity']} ({item['rarity']})\n"
                if len(materials) > 10:
                    mat_text += f"... 還有 {len(materials)-10} 種素材"
                embed.add_field(name="📦 素材", value=mat_text, inline=False)
            
            if consumables:
                con_text = ""
                for item in consumables[:10]:
                    con_text += f"🧪 **{item['item_name']}** x{item['quantity']}\n"
                if len(consumables) > 10:
                    con_text += f"... 還有 {len(consumables)-10} 種消耗品"
                embed.add_field(name="🧪 消耗品", value=con_text, inline=False)
            
            if equipment:
                eq_text = ""
                for item in equipment[:10]:
                    equip_emoji = "✅" if item['equipped'] else "📦"
                    eq_text += f"{equip_emoji} **{item['item_name']}** ({item['rarity']}) 耐久: {item['durability']}/{item['max_durability']}\n"
                if len(equipment) > 10:
                    eq_text += f"... 還有 {len(equipment)-10} 件裝備"
                embed.add_field(name="⚔️ 裝備", value=eq_text, inline=False)
        
        await interaction.followup.send(embed=embed)
    
    @group.command(name="use", description="使用物品")
    @app_commands.describe(item_id="物品ID")
    async def rpg_use(interaction: discord.Interaction, item_id: int):
        await interaction.response.defer()
        
        # 檢查物品是否存在
        item = await rpg.db.fetchrow(
            "SELECT * FROM rpg_inventory WHERE id = $1 AND user_id = $2 AND guild_id = $3",
            item_id, interaction.user.id, interaction.guild.id
        )
        
        if not item:
            await interaction.followup.send("❌ 找不到該物品")
            return
        
        # 這裡加入各種物品的使用邏輯
        await interaction.followup.send(f"✅ 使用 {item['item_name']} x1")
