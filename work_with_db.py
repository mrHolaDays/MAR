import config as cfg
import os
import create_case as core
import fork_with_cases_file as wfc
def create_db_file(db_name, cases_dir=cfg.CASES_DIR):
    with open(db_name, "wb") as f:
        f.write(cfg.DB_VERSION.to_bytes(cfg.DB_VERSION_BYTES, byteorder='big', signed=False)) #Версия
        f.write(b'\xf8')
        f.write(cases_dir.encode("utf-8"))
        f.write(b'\xfa')
        #Конфиг
        f.write(b'\x00'*24+b'\xfa')
        #Количество таблиц
        f.write(b'\x00'*cfg.MAX_TABLES_IN_BD_B)

def create_table(db_name, table_name, izm_names):
    with open(db_name, "rb+") as f:
        f.seek(cfg.DB_VERSION_BYTES+1)
        while True:
            n = f.read(1)
            if n==b'\xfa' or n==b'':
                break
        f.read(25)
        rm_pl = f.tell()
        tables_in_db = int.from_bytes(er := f.read(cfg.MAX_TABLES_IN_BD_B),byteorder='big', signed=False)+1
        f.seek(rm_pl)
        f.write(tables_in_db.to_bytes(cfg.MAX_TABLES_IN_BD_B,byteorder='big', signed=False))
        f.seek(0,2)
        f.write(tables_in_db.to_bytes(cfg.MAX_TABLES_IN_BD_B,byteorder='big', signed=False))
        f.write(table_name.encode("utf-8")+b'\xfa')
        f.write(len(izm_names).to_bytes(cfg.MAX_CASES_IN_TABLE_B,byteorder='big', signed=False))
        for i in range(len(izm_names)):
            f.write(i.to_bytes(cfg.MAX_CASES_IN_TABLE_B,byteorder='big', signed=False))
            f.write(izm_names[i].encode("utf-8")+b'\xfa')
        f.close()
        if not os.path.exists('config/'):
            os.mkdir('config/')
    wfc.create_cases_file(new_file_case := f"{table_name}_1.marc", tables_in_db, len(izm_names))
    with open(f"config/{table_name}.mart", 'wb') as f:
        f.write(tables_in_db.to_bytes(cfg.MAX_TABLES_IN_BD_B,byteorder='big', signed=False))
        f.write(new_file_case.encode("utf-8")+b'\xfa')
        f.close()


def pars_main_file(db_name):
    with open(db_name, "rb") as f:
        db_version = int.from_bytes(f.read(cfg.DB_VERSION_BYTES), byteorder='big', signed=False)
        cases_dir = []
        f.read(1)
        while True:
            n = f.read(1)
            if n==b'\xfa' or n==b'':
                break
            cases_dir.append(n.decode('utf-8'))
        cases_dir = ''.join(cases_dir)
        config = f.read(24)
        f.read(1)
        tables_plase = f.tell()
    return db_version, cases_dir, config, tables_plase

def pars_tables(db_name):
    pl = pars_main_file(db_name)[3]
    with open(db_name, "rb") as f:
        f.seek(pl)
        tables_in_db = int.from_bytes(f.read(cfg.MAX_TABLES_IN_BD_B),byteorder='big', signed=False)
        tables = {}
        for r in range(tables_in_db):
            f.read(cfg.MAX_TABLES_IN_BD_B)
            table_name = []
            while True:
                n = f.read(1)
                if n==b'\xfa' or n==b'' or n==b'\xf8':
                    break
                table_name.append(n.decode('utf-8'))
            table_name = ''.join(table_name)
            izm = {}
            izms = int.from_bytes(f.read(cfg.MAX_TABLES_IN_BD_B),byteorder='big', signed=False)
            for i in range(izms):
                f.read(cfg.MAX_TABLES_IN_BD_B)
                izm_name = []
                while True:
                    n = f.read(1)
                    if n==b'\xfa' or n==b'' or n==b'\xf8':
                        break
                    izm_name.append(n.decode('utf-8'))
                izm_name = ''.join(izm_name)
                izm[i] = izm_name
            tables[r] = (table_name, izms, izm)
        return tables_in_db, tables
    
def get_tables_files(db_name, table_name='all'):
    info = pars_tables(db_name)[1]
    if table_name == 'all':
        inf = {}
        for i in info:
            files = []
            with open(f'config/{info[i][0]}.mart', "rb") as f:
                f.read(cfg.MAX_TABLES_IN_BD_B)
                file_name = []
                while True:
                    n = f.read(1)
                    if n==b'\xfa':
                        file_name = ''.join(file_name)
                        files.append(file_name)
                    elif n==b'':
                        break
                    else:
                        file_name.append(n.decode('utf-8'))
            inf[i] = files
    elif type(table_name) == str:
        inf = {}
        for i in info:
            if info[i][0] == table_name:
                files = []
                with open(f'config/{info[i][0]}.mart', "rb") as f:
                    f.read(cfg.MAX_TABLES_IN_BD_B)
                    file_name = []
                    while True:
                        n = f.read(1)
                        if n==b'\xfa':
                            file_name = ''.join(file_name)
                            files.append(file_name)
                        elif n==b'':
                            break
                        else:
                            file_name.append(n.decode('utf-8'))
                inf[i] = files
                break
    elif type(table_name) == int:
        inf = {}
        files = []
        with open(f'config/{info[table_name][0]}.mart', "rb") as f:
            f.read(cfg.MAX_TABLES_IN_BD_B)
            file_name = []
            while True:
                n = f.read(1)
                if n==b'\xfa':
                    file_name = ''.join(file_name)
                    files.append(file_name)
                elif n==b'':
                    break
                else:
                    file_name.append(n.decode('utf-8'))
            inf[table_name] = files
    return inf

     
def find_case_in_table(db_name, table_name, cords):
    files = get_tables_files(db_name, table_name)
    result = False
    for i in files:
        for e in files[i]:
            if not result:
                result = wfc.find_case_in_f(e, cords)
            else: return result
    return result

def write_case_in_table(db_name, table_name, case_inp):
    files = get_tables_files(db_name, table_name)
    for i in files:
        for e in files[i]:
            if wfc.write_case_to_file(e, case_inp[0], case_inp[1]):
                return True
    return False

def get_all_cases_from_table(db_name, table_name):
    files = get_tables_files(db_name, table_name)
    cases = []
    if table_name == 'all':
        cases = {}
        for i in files:
            cas = []
            for e in files[i]:
                cas.append(wfc.read_all_cases_in_file(e))
            cases[i] = cas
    else:
        for i in files:
            for e in files[i]:
                cases.append(wfc.read_all_cases_in_file(e))
    return cases
    
db_n = 'main.marm'
tables = {'table1': ["IZm1", "izm2", 'izm3'], 'table2': ['izm12', 'izm22'], 'table3':["iz1","iz2","iz3","iz4"]}
create_db_file(db_n)
for i in tables:
    create_table(db_n, i, tables[i])

data_to_write = {'table1':[([123,2,-1],"TEST1"),([3,2,-1],"TEST2")],'table2':[([123,2],"TEST3"),([3,2],"TEST4")],'table3':[([123,2,-1,-21],"TEST5"),([3,2,-1,1],"TEST6")]}
for i in data_to_write:
    for e in data_to_write[i]:
        write_case_in_table(db_n, i,e)

print(get_all_cases_from_table(db_n, 'all'))