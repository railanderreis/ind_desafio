from datetime import datetime, timedelta
import pendulum
import prefect
from prefect import task, Flow
from prefect.schedules import CronSchedule
import pandas as pd

outts = './data/result/'

@task
def get_raw_data():
  path = './data/inputs/'
  return path

@task
def read_data_1(path):
  companies = pd.read_csv(path + "companies.tsv", sep="\t")
  return companies

@task
def read_data_2(path):
  sectors = pd.read_csv(path + "sectors.tsv", sep="\t")
  return sectors

@task
def read_data_3(path):
  deals = pd.read_csv(path + "deals.tsv", sep="\t")
  return deals 

@task
def read_data_4(path):
  contacts = pd.read_csv(path + "contacts.tsv", sep="\t")
  return contacts  

@task
def merge_d1(companies,sectors):
  comp_sector = pd.merge(companies[['companiesId','companiesName','sectorKey']],
                        sectors[['sectorKey','sector']],
                        on='sectorKey')
  return comp_sector

@task
def merge_d2(comp_sector,deals):
  comp_deals = pd.merge(comp_sector[['companiesId','companiesName','sectorKey','sector']],
                          deals[['dealsId','dealsDateCreated','dealsPrice','contactsId','companiesId']],
                          on='companiesId')
  return comp_deals

@task
def merge_d3(comp_deals,contacts):
  contacts = contacts.rename({' contactsId': 'contactsId'}, axis=1)
  deals_full = pd.merge(comp_deals[['companiesId','companiesName','sectorKey','sector','dealsId','dealsDateCreated',                                      'dealsPrice','contactsId']],
                          contacts[['contactsId','contactsName']],
                          on='contactsId', how='left')
  return deals_full

@task
def transfom_data(deals_full):
  deals_full['dealsDateCreated'] = pd.to_datetime(deals_full['dealsDateCreated'], format='%d-%m-Y',infer_datetime_format=True)
  return deals_full

@task
def separate_year(deals_full):
  deals_2017 = deals_full.loc[(deals_full['dealsDateCreated'].dt.year == 2017)]
  return (deals_2017)

@task
def consult_con(deals_2017):  
  vendas_contacts_2017 = deals_2017.groupby(deals_2017['contactsName'])['dealsPrice'].sum()
    
  return vendas_contacts_2017 

@task
def consult_mes(deals_2017):  
  vendas_mes_2017 = deals_2017.groupby(deals_2017['dealsDateCreated'].dt.month)['dealsPrice'].sum()
  
  return vendas_mes_2017

@task
def consult_sec(deals_2017):  
  vendas_sectors_2017 = deals_2017.groupby(deals_2017['sector'])['dealsPrice'].sum()
  
  vendas_sectors_o_2017 = pd.DataFrame(vendas_sectors_2017)
  vendas_sectors_o_2017.sort_values('dealsPrice', ascending=False)
  
  return vendas_sectors_o_2017 

'''
@task
def consult_2018(deals_2018): 
  vendas_contacts_2018 = deals_2018.groupby(deals_2018['contactsName'])['dealsPrice'].sum()
  vendas_mes_2018 = deals_2018.groupby(deals_2018['dealsDateCreated'].dt.month)['dealsPrice'].sum()
  vendas_sectors_2018 = deals_2018.groupby(deals_2018['sector'])['dealsPrice'].sum()
  vendas_sectors_o_2018 = pd.DataFrame(vendas_sectors_2018)
  vendas_sectors_o_2018.sort_values('dealsPrice', ascending=False)
  return consult_2018


@task
def consult_2019(deals_2019):  
  vendas_contacts_2019 = deals_2019.groupby(deals_2019['contactsName'])['dealsPrice'].sum()
  vendas_mes_2019 = deals_2019.groupby(deals_2019['dealsDateCreated'].dt.month)['dealsPrice'].sum()
  vendas_sectors_2019 = deals_2019.groupby(deals_2019['sector'])['dealsPrice'].sum()
  vendas_sectors_o_2019 = pd.DataFrame(vendas_sectors_2019)
  vendas_sectors_o_2019.sort_values('dealsPrice', ascending=False)
  return consult_2019

'''

@task
def output_tables(vendas_contacts_2017,vendas_mes_2017,vendas_sectors_o_2017):
  vendas_contacts_2017.to_csv(outts + "vendas_2017_contacts.xlsx")
  vendas_mes_2017.to_excel(outts + "vendas_2017_mes.xlsx")
  vendas_sectors_o_2017.to_csv(outts + "vendas_2017_sectors.xlsx")


with Flow('Desafio') as flow:
  path = get_raw_data()
  read_1 = read_data_1(path)
  read_2 = read_data_2(path)
  read_3 = read_data_3(path)
  read_4 = read_data_4(path)
  m_d1 = merge_d1(read_1,read_2)
  m_d2 = merge_d2(m_d1,read_3)
  m_d3 = merge_d3(m_d2,read_4)
  t_date = transfom_data(m_d3)
  sp_years = separate_year(t_date)

  c_con = consult_con(sp_years)
  c_mes = consult_mes(sp_years)
  c_sec = consult_sec(sp_years)

  outs = output_tables(c_con,c_mes,c_sec)



flow.register(project_name="desafio", idempotency_key=flow.serialized_hash())
flow.run_agent(token="7K--fskrjp9Fpz7wFUgJeg").start()