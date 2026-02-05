# %%
import pandas as pd
import frappe 
import numpy as np
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
    shipping_file=pd.read_excel(f'https://www.dropbox.com{path}')
    shipping_file.columns = shipping_file.iloc[shipping_file[shipping_file.columns[1]].dropna().index[0]]
    shipping_file.columns=shipping_file.columns.str.lower()
    shipping_file = shipping_file.iloc[shipping_file[shipping_file.columns[1]].dropna().index[0]+1:].reset_index(drop=True)
    master_data=pd.read_excel(f'https://www.dropbox.com{path}',sheet_name='master_data')
    shipping_file.etd=pd.to_datetime(shipping_file.etd,errors='coerce').dt.date
    shipping_file=shipping_file[shipping_file.etd>=pd.to_datetime('2000-01-01').date()]
    shipping_file=shipping_file[~shipping_file.etd.isna()].reset_index(drop=True)        
    shipping_file = shipping_file[headers]
    shipping_file=shipping_file.rename(columns=columns) # type: ignore
    master_data=master_data.fillna('') 
    # shipping_file=shipping_file.fillna("") 
    shipping_file[new_headers[1]]=shipping_file[new_headers[1]].astype(str).str.split('+').apply(lambda x: sum(int(i) for i in x if i.isdigit())).astype('Int16')
    shipping_file.pol=shipping_file.pol.astype(str)
    shipping_file.forwarder=shipping_file.forwarder.astype(str)
    # Normalize blank strings to NaN so fuzzy matching handles empties consistently
    shipping_file.replace("", pd.NA, inplace=True)
    for idx, row in shipping_file.iterrows():
        port_name = str(row['pol'])  # POL column
        best_match = None
        best_ratio = 0
        
        for _, row2 in master_data.iterrows():
            port_name2 = row2['pol']
            ratio = fuzz.ratio(port_name.lower().split('-')[0], port_name2.lower().strip().split('-')[0])
            
            if ratio > best_ratio:  
                best_ratio = ratio
                best_match = port_name2
        
        if best_ratio > 70:  
            shipping_file.at[idx, 'pol'] = best_match
            # data1.at[idx, 'fuzzy%'] = best_ratio
        else:
            # Keep missing matches as NaN for pandas compatibility across versions
            shipping_file.at[idx, 'pol'] = pd.NA

    for idx, row in shipping_file.iterrows():
        shipping_line = str(row['liner']) # Shipping Line column
        best_match = None
        best_ratio = 0
        for _, row2 in master_data.iterrows():
            shipping_line2 = row2['liner']
            ratio = fuzz.ratio(shipping_line.lower().split('/')[0], shipping_line2.lower().strip().split('-')[0])
            if ratio > best_ratio:  
                best_ratio = ratio
                best_match = shipping_line2
        if best_ratio > 70:  
            shipping_file.at[idx, 'liner'] = best_match
            # shipping_file.at[idx, 'fuzzy%'] = best_ratio
        else:
            shipping_file.at[idx, 'liner'] = pd.NA
    for idx, row in shipping_file.iterrows():
        forwarder = str(row['forwarder']) # Forwarder column
        best_match = None
        best_ratio = 0
        for _, row2 in master_data.iterrows():
            forwarder2 = str(row2['forwarder'])
            ratio = fuzz.ratio(forwarder.lower().strip().split('-')[0], forwarder2.lower().strip().split('-')[0])
            if ratio > best_ratio:  
                best_ratio = ratio
                best_match = forwarder2
        
        if best_ratio > 70:  
            shipping_file.at[idx, 'forwarder'] = best_match
        else:
            shipping_file.at[idx, 'forwarder'] = pd.NA        

    shipping_file.reset_index(drop=True,inplace=True)
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

            # Normalize to string for text checks; handle NaN/None safely
            if pd.isna(cell):
                shipping_file.at[index, 'freight_per_cntr'] = 0
                continue

            cell_str = str(cell).strip()

            # Ignore incoterms like CFR / FOB and blanks -> 0
            if cell_str.upper() in ('CFR', 'FOB', ''):
                shipping_file.at[index, 'freight_per_cntr'] = str(0)
                continue

            # If we have a value in parentheses, try to use that
            if '(' in cell_str and ')' in cell_str:
                value_in_parens = cell_str.split('(')[1].split(')')[0].strip()
                if value_in_parens.isdigit():
                    shipping_file.at[index, 'freight_per_cntr'] = int(value_in_parens)
                    continue

            # Fallback: if the cleaned text is a pure number, use it, else 0
            numeric_candidate = cell_str.replace(',', '')
            if numeric_candidate.replace('.', '', 1).isdigit():
                shipping_file.at[index, 'freight_per_cntr'] = float(numeric_candidate)
            else:
                shipping_file.at[index, 'freight_per_cntr'] = str(0)
      
            # try:
            #     if cell.isdigit():
            #         print(value_in_parens)
            #         shipping_file.at[index, 'freight_per_cntr'] = int(value_in_parens)
            #     elif value_in_parens.isdigit():
            #         value_in_parens = cell.split('(')[1].split(')')[0]
            #         shipping_file.at[index, 'freight_per_cntr'] = int(value_in_parens)
            #     else:
            #         shipping_file.at[index, 'freight_per_cntr'] = str(0)
            # except Exception as e:
            #         print(e)
            #         shipping_file.at[index, 'freight_per_cntr'] = str(0)
    
    shipping_file.cntr_vol=shipping_file.cntr_vol.replace('',0) 
    shipping_file.docs_received=shipping_file.docs_received.replace('',0) 
    shipping_file.arrived=shipping_file.arrived.replace('',0) 
    shipping_file.cntr_returned=shipping_file.cntr_returned.replace('',0) 
    shipping_file.free_time=shipping_file.free_time.replace('',0) 
    shipping_file.freight_per_cntr=shipping_file.freight_per_cntr.replace('',0) 

    shipping_file.freight_per_cntr=shipping_file.freight_per_cntr.astype(float)
    shipping_file.arrived=shipping_file.arrived.astype(int)
    shipping_file.cntr_returned=shipping_file.cntr_returned.astype(int)
    shipping_file.docs_received=shipping_file.docs_received.astype(int)
    shipping_file.arrival_date=shipping_file.arrival_date.astype(str).str[:10]
    shipping_file.shipping_date=shipping_file.shipping_date.astype(str).str[:10]

    # Convert all remaining NaN/NaT values to Python None
    # so the returned dict has None instead of NaN
    shipping_file = shipping_file.where(pd.notna(shipping_file), None)
    # shipping_file=shipping_file.replace(np.nan,None)


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
