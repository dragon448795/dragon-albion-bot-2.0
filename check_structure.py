# check_structure.py
import os
import importlib.util

def check_file(file_path, required_classes=None):
    """檢查檔案是否存在且包含必要的類別"""
    if not os.path.exists(file_path):
        return False, f"檔案不存在: {file_path}"
    
    if required_classes:
        try:
            spec = importlib.util.spec_from_file_location("module", file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            missing_classes = []
            for cls in required_classes:
                if not hasattr(module, cls):
                    missing_classes.append(cls)
            
            if missing_classes:
                return False, f"缺少類別: {', '.join(missing_classes)}"
            
            return True, "✓ 完整"
        except Exception as e:
            return False, f"導入錯誤: {e}"
    
    return True, "✓ 存在"

def main():
    print("🔍 檢查 RPG 系統結構")
    print("=" * 50)
    
    files_to_check = [
        ("database.py", ["RPGDatabase"]),
        ("monsters.py", ["MonsterSystem"]),
        ("items.py", ["ItemSystem"]),
        ("combat.py", ["CombatSystem"]),
        ("party.py", ["PartySystem"]),
    ]
    
    all_good = True
    for filename, required_classes in files_to_check:
        exists, message = check_file(filename, required_classes)
        status = "✅" if exists else "❌"
        print(f"{status} {filename}: {message}")
        if not exists:
            all_good = False
    
    print("\n" + "=" * 50)
    if all_good:
        print("🎉 所有必要檔案都存在！")
        print("下一步：建立整合系統")
    else:
        print("⚠️ 部分檔案有問題，請先修復")

if __name__ == "__main__":
    main()
