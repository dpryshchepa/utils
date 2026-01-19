import os
import yaml
import sys
import argparse
from collections import defaultdict, Counter

def find_yaml_files(root_dir):
    yaml_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.yml') or file.endswith('.yaml'):
                yaml_files.append(os.path.join(root, file))
    return yaml_files

def get_indent(line):
    return len(line) - len(line.lstrip())

def find_child_block(lines, parent_idx, key=None, name=None):
    """
    Finds a child block inside parent_idx.
    If key is provided, looks for 'key:'.
    If name is provided, looks for '- name: name'.
    Returns the line index or -1.
    """
    if parent_idx >= len(lines):
        return -1
        
    parent_indent = get_indent(lines[parent_idx])
    
    for i in range(parent_idx + 1, len(lines)):
        line = lines[i]
        stripped = line.strip()
        
        # Skip empty lines and comments
        if not stripped or stripped.startswith('#'):
            continue
            
        curr_indent = get_indent(line)
        
        # If we went back to parent level or higher, we failed to find it in the block
        if curr_indent <= parent_indent:
            return -1
            
        if key and stripped.startswith(f"{key}:"):
            return i
            
        if name and stripped.startswith("- name:"):
            val = stripped.split(':', 1)[1].strip()
            # Remove quotes
            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]
            if val == name:
                return i
                
    return -1

def update_file(file_path, fixes):
    """
    fixes: list of dicts with keys: type, parent, table, col, new_desc
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    # Sort fixes by reverse order of appearance? No, we need to find them first.
    # Since we are modifying lines, indices might shift if we insert lines.
    # But we are mostly replacing. If we insert, we need to handle shifts.
    # Simplest is to apply one by one and re-search, or track shifts.
    # Or just read fresh for each fix? That's slow but safe.
    # Given small file count, let's read fresh or be careful.
    # Actually, if we just replace lines, indices don't shift.
    # If we insert 'description:', they shift.
    
    # Let's try to do it in one pass or re-read. Re-reading is safer for MVP.
    
    # Group fixes by file is already done by caller.
    # We will apply fixes sequentially.
    
    modified = False
    
    for fix in fixes:
        # We need to re-scan because line numbers might have changed if we inserted
        # But to avoid infinite loop or complexity, let's just scan.
        
        # 1. Find root key (models or sources)
        root_key = 'models' if fix['type'] == 'model' else 'sources'
        
        # Scan for "models:" or "sources:" at top level (indent 0 usually, or just find it)
        root_idx = -1
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{root_key}:"):
                root_idx = i
                break
        
        if root_idx == -1:
            print(f"  Warning: Could not find '{root_key}:' in {file_path}")
            continue
            
        # 2. Find parent (model/source name)
        parent_idx = find_child_block(lines, root_idx, name=fix['parent'])
        if parent_idx == -1:
            print(f"  Warning: Could not find {fix['type']} '{fix['parent']}' in {file_path}")
            continue
            
        current_idx = parent_idx
        
        # 3. If source, find table
        if fix['type'] == 'source':
            tables_idx = find_child_block(lines, current_idx, key='tables')
            if tables_idx == -1:
                print(f"  Warning: Could not find 'tables:' in {file_path}")
                continue
            
            table_idx = find_child_block(lines, tables_idx, name=fix['table'])
            if table_idx == -1:
                print(f"  Warning: Could not find table '{fix['table']}' in {file_path}")
                continue
            current_idx = table_idx
            
        # 4. Find columns
        cols_idx = find_child_block(lines, current_idx, key='columns')
        if cols_idx == -1:
            print(f"  Warning: Could not find 'columns:' in {file_path}")
            continue
            
        # 5. Find column
        col_idx = find_child_block(lines, cols_idx, name=fix['col'])
        if col_idx == -1:
            print(f"  Warning: Could not find column '{fix['col']}' in {file_path}")
            continue
            
        # 6. Find description
        desc_idx = find_child_block(lines, col_idx, key='description')
        
        escaped_desc = fix['new_desc'].replace('"', '\\"')
        new_desc_str = f'"{escaped_desc}"'
        
        if desc_idx != -1:
            # Replace existing
            line = lines[desc_idx]
            key_part = line.split(':', 1)[0]
            # Preserve comment if any?
            # If line has #, keep it? Too complex. Just replace value.
            lines[desc_idx] = f"{key_part}: {new_desc_str}\n"
            modified = True
        else:
            # Insert new
            col_line = lines[col_idx]
            indent = get_indent(col_line)
            # Assume standard indent increase (2 spaces)
            prefix = " " * (indent + 2)
            lines.insert(col_idx + 1, f"{prefix}description: {new_desc_str}\n")
            modified = True
            
    if modified:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print(f"Updated {file_path}")

def check_and_fix(dbt_root, fix_mode=False):
    models_dir = os.path.join(dbt_root, 'models')
    if not os.path.exists(models_dir):
        print(f"Error: Directory not found: {models_dir}")
        return

    files = find_yaml_files(models_dir)
    print(f"Scanning {len(files)} files in {models_dir}...")

    # Data structure:
    # col_name -> { description -> [ {file, type, parent, table} ] }
    col_map = defaultdict(lambda: defaultdict(list))

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            if not data: continue

            if 'models' in data:
                for model in data['models']:
                    m_name = model.get('name')
                    for col in model.get('columns', []):
                        c_name = col.get('name')
                        # Treat None as empty string
                        desc = col.get('description') or ''
                        if c_name:
                            col_map[c_name][desc].append({
                                'file': file_path,
                                'type': 'model',
                                'parent': m_name,
                                'table': None
                            })

            if 'sources' in data:
                for source in data['sources']:
                    s_name = source.get('name')
                    for table in source.get('tables', []):
                        t_name = table.get('name')
                        for col in table.get('columns', []):
                            c_name = col.get('name')
                            desc = col.get('description') or ''
                            if c_name:
                                col_map[c_name][desc].append({
                                    'file': file_path,
                                    'type': 'source',
                                    'parent': s_name,
                                    'table': t_name
                                })

        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Analyze inconsistencies
    fixes_by_file = defaultdict(list)
    inconsistent_count = 0
    
    for col, desc_dict in col_map.items():
        # Filter out empty descriptions from being the "winner"
        non_empty_descs = {d: locs for d, locs in desc_dict.items() if d}
        
        # If all are empty, or only one unique non-empty description exists and no empty ones (wait, if 1 non-empty and 0 empty, len is 1. If 1 non-empty and 5 empty, len is 2)
        # We want to act if:
        # 1. More than 1 non-empty description (conflict)
        # 2. 1 non-empty description AND 1+ empty descriptions (fill missing)
        
        # If no non-empty descriptions, we can't fix anything.
        if not non_empty_descs:
            continue
            
        # If we have > 1 unique description (including empty), we have work to do.
        if len(desc_dict) > 1:
            inconsistent_count += 1
            
            # Pick best from NON-EMPTY candidates
            sorted_descs = sorted(non_empty_descs.items(), key=lambda x: len(x[1]), reverse=True)
            best_desc = sorted_descs[0][0]
            
            print(f"\nInconsistency for '{col}':")
            print(f"  Best description ({len(sorted_descs[0][1])} uses): {repr(best_desc)}")
            
            # Iterate over ALL descriptions (including empty) to find victims
            for desc, occurrences in desc_dict.items():
                if desc == best_desc:
                    continue
                    
                print(f"  Other description ({len(occurrences)} uses): {repr(desc)}")
                for occ in occurrences:
                    print(f"    - {occ['file']} ({occ['type']}: {occ['parent']}{'.' + occ['table'] if occ['table'] else ''})")
                    
                    if fix_mode:
                        fixes_by_file[occ['file']].append({
                            'type': occ['type'],
                            'parent': occ['parent'],
                            'table': occ['table'],
                            'col': col,
                            'new_desc': best_desc
                        })

    if inconsistent_count == 0:
        print("\nNo inconsistencies found!")
    else:
        print(f"\nFound {inconsistent_count} columns with inconsistent descriptions.")
        
    if fix_mode and fixes_by_file:
        print("\nApplying fixes...")
        for file_path, fixes in fixes_by_file.items():
            try:
                update_file(file_path, fixes)
            except Exception as e:
                print(f"Failed to update {file_path}: {e}")
        print("Fixes applied.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check and fix dbt column description inconsistencies.')
    parser.add_argument('dir', nargs='?', default='dbt', help='Directory to scan (default: dbt)')
    parser.add_argument('--fix', action='store_true', help='Auto-fix inconsistencies by using the most frequent description')
    
    args = parser.parse_args()
    
    target_dir = args.dir
    if not os.path.exists(target_dir):
         if os.path.exists(os.path.join(os.getcwd(), 'models')):
             target_dir = os.getcwd()
             
    check_and_fix(target_dir, args.fix)
