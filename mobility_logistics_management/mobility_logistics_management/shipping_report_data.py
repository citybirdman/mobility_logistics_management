# %%
import pandas as pd
import frappe 
from rapidfuzz import fuzz, process

@frappe.whitelist()
def get_data():
    columns={
        'order no.': 'title', # Mandatory0
        'vol.': 'cntr_vol', # 01
        'type': 'cntr_type', # None02
        'docs':'docs_received', # 03
        'shipping line': 'liner', # None04
        'forwarder': 'forwarder', # None05
        'pol': 'pol', # None06
        'pod': 'pod', # None07
        'etd': 'shipping_date', # Mandatory08
        'eta': 'arrival_date', # None09
        'ata': 'arrived', # 010
        'return': 'cntr_returned', # 011
        'f/t': 'free_time', # 012
        'bol no.': 'bol_no', # None 13
        'freight/cntr': 'freight_per_cntr', # 014
        'incoterm':'incoterm' # None15
    }
    headers=list(columns.keys())
    new_headers=list(columns.values())
    link=frappe.db.sql("SELECT value FROM `tabSingles` WHERE doctype = 'Logistics Management Settings' AND field = 'shipping_report_dropbox_shared_uri_path'", as_dict=True)
    path=link[0]['value']
    shipping_file=pd.read_excel(f'https://www.dropbox.com/scl/fi/8opxja4ddlygpqm1dniqp/ALMOMTAZ-SHIPPING-REPORT-2026.xlsx?rlkey=zdupycob4e0ykkdc3j3v24fb2&dl=1')
    shipping_file.columns = shipping_file.iloc[shipping_file[shipping_file.columns[1]].dropna().index[0]]
    shipping_file.columns=shipping_file.columns.str.lower()
    shipping_file = shipping_file.iloc[shipping_file[shipping_file.columns[1]].dropna().index[0]+1:].reset_index(drop=True)
    master_data=pd.read_excel(f'https://www.dropbox.com/scl/fi/8opxja4ddlygpqm1dniqp/ALMOMTAZ-SHIPPING-REPORT-2026.xlsx?rlkey=zdupycob4e0ykkdc3j3v24fb2&dl=1',sheet_name='master_data')
    shipping_file.etd=pd.to_datetime(shipping_file.etd,errors='coerce').dt.date
    shipping_file=shipping_file[shipping_file.etd>=pd.to_datetime('2000-01-01').date()]
    shipping_file=shipping_file[~shipping_file.etd.isna()].reset_index(drop=True)        
    shipping_file = shipping_file[headers]
    shipping_file=shipping_file.rename(columns=columns) # type: ignore
    master_data=master_data.fillna('') 
    shipping_file=shipping_file.fillna('') 
    shipping_file[new_headers[1]]=shipping_file[new_headers[1]].astype(str).str.split('+').apply(lambda x: sum(int(i) for i in x if i.isdigit())).astype('Int16')
    print(master_data.head(20))
    for idx, row in shipping_file.iterrows():
        port_name = row[new_headers[6]]  # POL column
        best_match = None
        best_ratio = 0
        
        for _, row2 in master_data.iterrows():
            port_name2 = row2['pol'].strip().replace(' ','-')
            ratio = fuzz.ratio(port_name.lower().split('/')[0], port_name2.lower().strip().split('-')[0])
            
            if ratio > best_ratio:  
                best_ratio = ratio
                best_match = port_name2
        
        if best_ratio > 70:  
            shipping_file.at[idx, new_headers[6]] = best_match
            # data1.at[idx, 'fuzzy%'] = best_ratio
        else:
            shipping_file.at[idx, new_headers[6]] = None  
    for idx, row in shipping_file.iterrows():
        shipping_line = row[new_headers[4]].strip().replace(' ','-') # Shipping Line column
        best_match = None
        best_ratio = 0
        for _, row2 in master_data.iterrows():
            shipping_line2 = row2['liner']
            ratio = fuzz.ratio(shipping_line.lower().split('/')[0], shipping_line2.lower().strip().split('-')[0])
            if ratio > best_ratio:  
                best_ratio = ratio
                best_match = shipping_line2
            print(shipping_line,shipping_line2)
        if best_ratio > 70:  
            shipping_file.at[idx, new_headers[4]] = best_match
            # shipping_file.at[idx, 'fuzzy%'] = best_ratio
        else:
            shipping_file.at[idx, new_headers[4]] = None
    for idx, row in shipping_file.iterrows():
        forwarder = row[new_headers[5]].strip().replace(' ','-') # Forwarder column
        best_match = None
        best_ratio = 0
        for _, row2 in master_data.iterrows():
            forwarder2 = row2['forwarder']
            ratio = fuzz.ratio(forwarder.lower().strip().split('/')[0], forwarder2.lower().strip().split('-')[0])
            if ratio > best_ratio:  
                best_ratio = ratio
                best_match = forwarder2
        
        if best_ratio > 70:  
            shipping_file.at[idx, new_headers[5]] = best_match
        else:
            shipping_file.at[idx, new_headers[5]]=None           


    for index, row in shipping_file.iterrows():
        if row['docs_received']=='#':
            shipping_file.at[index, 'docs_received'] = str(1)
        else:
            shipping_file.at[index, 'docs_received'] = str(0)
        
        if row['cntr_returned']=='#':
            shipping_file.at[index, 'cntr_returned'] = str(1)
        else:
            shipping_file.at[index, 'cntr_returned'] = str(0)   
            
    for index ,row in shipping_file.iterrows():
            if row['arrival_date']=='Arrived':
                shipping_file.at[index, 'arrival_date'] = row['arrived']
                shipping_file.at[index, 'arrived'] = str(1)
            else:
                shipping_file.at[index, 'arrived'] = str(0)


    for index, row in shipping_file.iterrows():
            cell = row['freight_per_cntr']
            print(cell)
            try:
                value_in_parens = cell.split('(')[1].split(')')[0]
                if value_in_parens.isdigit():
                    shipping_file.at[index, 'freight_per_cntr'] = value_in_parens
                else:
                    shipping_file.at[index, 'freight_per_cntr'] = 0
            except (IndexError, AttributeError):
                    shipping_file.at[index, 'freight_per_cntr'] = 0
    
    shipping_file[new_headers[1]]=shipping_file[new_headers[1]].replace('',0) 
    shipping_file[new_headers[3]]=shipping_file[new_headers[3]].replace('',0) 
    shipping_file[new_headers[10]]=shipping_file[new_headers[10]].replace('',0) 
    shipping_file[new_headers[11]]=shipping_file[new_headers[11]].replace('',0) 
    shipping_file[new_headers[12]]=shipping_file[new_headers[12]].replace('',0) 
    shipping_file[new_headers[14]]=shipping_file[new_headers[14]].replace('',0) 
    shipping_file.replace('',None,inplace=True)

    shipping_file[new_headers[14]]=shipping_file[new_headers[14]].astype(float)
    shipping_file[new_headers[10]]=shipping_file[new_headers[10]].astype(int)
    shipping_file[new_headers[11]]=shipping_file[new_headers[11]].astype(int)
    shipping_file[new_headers[3]]=shipping_file[new_headers[3]].astype(int)
    shipping_file.arrival_date=pd.to_datetime(shipping_file.arrival_date, errors='coerce').dt.date
    shipping_file.shipping_date=pd.to_datetime(shipping_file.shipping_date, errors='coerce').dt.date
    print(shipping_file.info())
    print(shipping_file.head())
    return shipping_file.to_dict(orient='records')

 


@frappe.whitelist()
def Update_shipping_report_data():
    settings = frappe.get_doc("Logistics Management Settings")
    try:
        logs = get_data()
        for log in logs:
            frappe.db.set_value("Purchase Invoice", {"title": log['title']}, log)
        settings.latest_successful_sync_date = settings.latest_sync_date = frappe.utils.now_datetime()
        settings.error_log = ""
        settings.save()
    except Exception as e:
        frappe.log_error(title="Error while fetching shipping report data.", message=f"{e}")
        settings.latest_sync_date = frappe.utils.now_datetime()
        settings.error_log = e
        settings.save()
# %%
