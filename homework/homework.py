"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import zipfile
import pandas as pd
import os

input_dir = 'files/input'
output_dir = 'files/output'

os.makedirs(output_dir, exist_ok=True)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerles un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cambiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    df_all= pd.DataFrame()

    for filename in os.listdir(input_dir):

        if filename.endswith('.zip'):

            zip_path = os.path.join(input_dir,filename)


            with zipfile.ZipFile(zip_path, 'r') as zip_ref:

                csv_file = zip_ref.namelist()[0]

                    
                with zip_ref.open(csv_file) as my_file:

                    df_temp = pd.read_csv(my_file)
                    df_all = pd.concat([df_all,df_temp], ignore_index=True)

    return df_all

df_client = clean_campaign_data()[['client_id','age','job','marital','education','credit_default', 'mortgage'].copy()]
df_client['job'] = df_client['job'].str.replace('.','').str.replace('-','_')
df_client['education'] = df_client['education'].replace('unknow',pd.NA).str.replace('.','_')
df_client['credit_default'] = df_client ['credit_default'].map({'yes':'1', 'no':'0'})
df_client['mortgage'] = df_client['mortgage'].map({'yes':'1','no':'2'})

df_campaign = clean_campaign_data()[['client_id','number_contacts','contact_duration','previous_campaign_contacts','previous_outcome','campaign_outcome','day', 'month'].copy()]

df_campaign['previous_outcome'] = df_campaign['previous_outcome'].replace('success',1)
df_campaign['previous_outcome'] = df_campaign['previous_outcome'].apply(lambda x: 0 if x != 1 else x)
df_campaign['campaign_outcome'] = df_campaign['campaign_outcome'].replace('yes',1)
df_campaign['campaign_outcome'] = df_campaign['campaign_outcome'].apply(lambda x: 0 if x != 1 else x)
df_campaign['last_contact_date'] = pd.to_datetime('2022 '+ df_campaign['month']+' '+ df_campaign['day'].astype(str))
df_campaign['last_contact_date'] = df_campaign['last_contact_date'].dt.strftime('%Y-%m-%d')
df_campaign = df_campaign [['client_id','number_contacts','contact_duration','previous_campaign_contacts','previous_outcome','campaign_outcome','last_contact_date'].copy()]
df_economics = clean_campaign_data()[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()

df_client.to_csv('files/output/client.csv', index=False)
df_campaign.to_csv('files/output/campaign.csv', index=False)
df_economics.to_csv('files/output/economics.csv', index=False)

if __name__ == "__main__":
    clean_campaign_data()
