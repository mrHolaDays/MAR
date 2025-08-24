import create_case as core
import config
import os
import time
FRESPASE = {}

def create_cases_file(file_name,table_id,cords,cord_s = config.STANDART_CORD_SIZE,pl_in_f=config.BYTES_PLASE_IN_FILE,cases_in_file=config.CASES_IN_FILE, cases_dir=config.CASES_DIR, tables_in_bd = config.MAX_TABLES_IN_BD_B, max_cases_in_db = config.MAX_CASES_IN_TABLE_B, len_size_b = config.STANDART_LEN_SIZE+1):
    if not os.path.exists(cases_dir[:-1]):
        os.mkdir(cases_dir[:-1])
    with open(cases_dir+file_name, "wb") as f:
        f.write(int(table_id).to_bytes(tables_in_bd, byteorder='big', signed=False))
        f.write(cords.to_bytes(config.MAX_TABLES_IN_BD_B, byteorder='big', signed=False))
        f.write(int(cases_in_file).to_bytes(max_cases_in_db, byteorder='big', signed=False))
        f.write(b'\x00'*max_cases_in_db)
        for i in range(cases_in_file):
            f.write(b'\x00'*(cords*cord_s)+b'\x00'*pl_in_f+b'\x00'*len_size_b)

def get_table_id(file_name, cases_dir=config.CASES_DIR, tables_in_bd = config.MAX_TABLES_IN_BD_B):
    with open(cases_dir+file_name, "rb") as f:
        return int.from_bytes(f.read(tables_in_bd),  byteorder='big', signed=False)
def get_tables_col_inf(file_name, cases_dir=config.CASES_DIR, tables_in_bd = config.MAX_TABLES_IN_BD_B, max_cases_in_db = config.MAX_CASES_IN_TABLE_B):
    with open(cases_dir+file_name, "rb") as f:
        f.seek(tables_in_bd+config.MAX_CASES_IN_TABLE_B)
        data = int.from_bytes(f.read(max_cases_in_db),  byteorder='big', signed=False)
        data_f = int.from_bytes(f.read(max_cases_in_db),  byteorder='big', signed=False)
        return data, data_f, data-data_f
    
def get_cases_cords_inf(file_name, cases_dir=config.CASES_DIR,cord_s = config.STANDART_CORD_SIZE, tables_in_bd = config.MAX_TABLES_IN_BD_B,pl_in_f=config.BYTES_PLASE_IN_FILE, max_cases_in_db = config.MAX_CASES_IN_TABLE_B, len_size_b = config.STANDART_LEN_SIZE+1):
    sdata = get_tables_col_inf(file_name)
    start_cord = tables_in_bd+max_cases_in_db+max_cases_in_db+config.MAX_CASES_IN_TABLE_B
    if sdata[1] == 0:
        return start_cord, (), 0
    else:  
        with open(cases_dir+file_name, "rb") as f:
            f.seek(tables_in_bd)
            cords = int.from_bytes(f.read(config.MAX_CASES_IN_TABLE_B),byteorder='big', signed=False)
            f.seek(start_cord)
            case_list = []
            for i in range(sdata[1]):
                cords_case = core.unpuk_cords(f.read(cords*cord_s), cord_val=cords)[0]
                rw_pl = f.tell()
                start_pl = int.from_bytes(f.read(pl_in_f),byteorder='big', signed=False)
                case_len = int.from_bytes(f.read(len_size_b),byteorder='big', signed=False)   
                case_list.append([cords_case, start_pl,case_len, rw_pl])
            return f.tell(), case_list, cords
def write_case_to_file(file_name,cord ,data, cases_dir=config.CASES_DIR,pl_in_f=config.BYTES_PLASE_IN_FILE,len_size_b = config.STANDART_LEN_SIZE+1, tables_in_bd = config.MAX_TABLES_IN_BD_B, max_cases_in_db = config.MAX_CASES_IN_TABLE_B):
    cord_pl = get_cases_cords_inf(file_name)
    data_bytes = core.create_case(cord, data)
    cord_block = core.create_cord_block(cord)
    existed = 0
    if (cord_pl[1]==()):
        existed = False
    for i in cord_pl[1]:
        if i[0] == cord:
            existed = True
            st_pl = i[1]
            sz = i[2]
            cs_pl = i[3]
            break
    if (cord_pl[1]==()) or not(existed):
        with open(cases_dir+file_name, "rb+") as f:
            f.seek(0, 2)
            plase_to_write = f.tell()
            f.seek(cord_pl[0])
            f.write(cord_block+plase_to_write.to_bytes(pl_in_f, byteorder='big', signed=False)+len(data_bytes).to_bytes(len_size_b, byteorder='big', signed=False))
            f.seek(plase_to_write)
            f.write(data_bytes)
            f.seek(tables_in_bd+config.MAX_TABLES_IN_BD_B+max_cases_in_db)
            f.write((len(cord_pl[1])+1).to_bytes(max_cases_in_db, byteorder='big', signed=False))
            return True
    else:
        if sz>=len(data_bytes):
            with open(cases_dir+file_name, "rb+") as f:
                f.seek(st_pl)
                f.write(data_bytes)
                return True
        else:
            global FRESPASE
            fr_sp = False
            if FRESPASE!={}:
                for i in FRESPASE:
                    if i>len(data_bytes):
                        fr_sp = True
                        plase_to_write =  min(FRESPASE[i])
                        break
            if True:
                with open(cases_dir+file_name, "rb+") as f:
                    f.seek(0, 2)
                    if not(fr_sp):
                        plase_to_write = f.tell()
                        return True
                    f.write(data_bytes)
                    f.seek(cs_pl)
                    f.write(plase_to_write.to_bytes(pl_in_f, byteorder='big', signed=False)+len(data_bytes).to_bytes(len_size_b, byteorder='big', signed=False))
                    if not sz in FRESPASE: FRESPASE[sz] = [st_pl]
                    else: FRESPASE[sz].append(st_pl)
                    return True
                
            
def find_case_in_f(file_name,cord,cases_dir=config.CASES_DIR, unpaked=True):
    all_cases = get_cases_cords_inf(file_name)[1]
    case_is_defind = False
    for i in all_cases:
        if cord == i[0]:
            case_is_defind = True
            with open(cases_dir+file_name, "rb") as f:
                f.seek(i[1])
                if unpaked:
                    return core.un_puck_case(f.read(i[2]), cords_val=len(cord))
                else:
                    return f.read(i[2])
    if not case_is_defind:
        return False
        
    
def read_all_cases_in_file(file_name,cases_dir=config.CASES_DIR, unpaked=True):
    asd, all_cases, cord = get_cases_cords_inf(file_name)
    cases = []
    with open(cases_dir+file_name, "rb") as f:
        for i in all_cases:
            f.seek(i[1])
            if unpaked:
                cases.append(core.un_puck_case(f.read(i[2]), cords_val=cord))
            else:
                cases.append(f.read(i[2]))
    return cases

def clean_pars_file(file_name,cases_dir=config.CASES_DIR):
    create_cases_file(f'parsing_{file_name}',1, 3)
    asd, all_cords, cord= get_cases_cords_inf(file_name)
    with open(cases_dir+file_name, "rb") as f:
        for i in all_cords:
            f.seek(i[1])
            case_now = core.un_puck_case(f.read(i[2]), cords_val=cord)
            write_case_to_file(f'parsing_{file_name}', case_now[0][0], case_now[3])
    f.close()
    os.remove(f'{cases_dir}{file_name}')
    os.rename(f'{cases_dir}parsing_{file_name}', f'{cases_dir}{file_name}')


# file_name = "test1.marc"
# create_cases_file("test1.marc", 1, 3)
# write_case_to_file(file_name, [123,12,-1], "TEST1")
# write_case_to_file(file_name, [128,12,-1], "TEST2")
# write_case_to_file(file_name, [123,12,1], "TEST4")
# write_case_to_file(file_name, [123,12,-1], "TEST3")
# write_case_to_file(file_name, [123,12,1], "TEST4")
# write_case_to_file(file_name, [123,12,1], "TEST4")
# write_case_to_file(file_name, [123,12,-1], "TEST11231123123123123123123123123")
# write_case_to_file(file_name, [128,12,-1], "TEST22312")
# write_case_to_file(file_name, [123,12,1], "TEST4")
# write_case_to_file(file_name, [123,12,1], "TEST4")

# write_case_to_file(file_name, [123,12,1], "TEST4")
# print(get_cases_cords_inf(file_name))
# print(find_case_in_f(file_name, [123,12,-1]))
# clean_pars_file(file_name)

# print(read_all_cases_in_file(file_name))