#!/usr/bin/env python
# coding: utf-8


import ipywidgets as widgets
import sys
import getpass
import ktools.modules
from ktools.modules import auth, uat_dashboard, pyspark_profile, kt

import IPython

class Dashboards:
    out = widgets.Output(layout={'border': '1px solid black'})
    
    def __init__(self):
        self.spark_obj = self.spark_check()
        
        

    @out.capture(clear_output = True)    
    def spark_check(self):
        pwd = getpass.getpass()
        spark_obj = auth.Auth(pwd)
        check_password = spark_obj.check_pass()
        return spark_obj
        

   ########################### WIDGETS ###########################################     
    def comboBox(self):
        # Put the options in a list and call that list (to avoid duplication below)
        self.dropdown_dashboardSelect = widgets.Dropdown(
        placeholder='Select a Dashboard',
        options= [" ", "Data Refinery Dashboard", "Copy File Dashboard", "Create Virtual Environment"],
        description='Dashboards:',
        )
    
        self.button_authenticate = widgets.Button(
            description='Authenticate',
            button_style='warning',
            icon='eye',
            layout={'margin': '0 80px 0 0'}
        )
    ##############################################################################
    
    @out.capture(clear_output = True)
    def update_dashboard(self, *args):
        if self.dropdown_dashboardSelect.value == 'Data Refinery Dashboard':
            from ktools.modules.uat_dashboard import Parquet_Dashobard
            usr = getpass.getuser()
            if self.button_authenticate.button_style != 'success':
                Parquet_Dashobard(usr, ' ').main_code()
            else:
                Parquet_Dashobard(usr, spark).main_code()
        elif self.dropdown_dashboardSelect.value == 'Copy File Dashboard':
            from ktools.modules.file_explorer import File_Explorer
            File_Explorer().main_code()
        elif self.dropdown_dashboardSelect.value == 'Create Virtual Environment':
            from ktools.modules.create_env import Create_env
            Create_env().main_code()
        elif self.dropdown_dashboardSelect.value == 'Github':
            display("REMOVED")
        else:
            pass
    
    @out.capture(clear_output = True)
    def update_auth(self, *args):
        authentication = self.spark_obj.authenticate()
        if authentication is None:
            display("Please execute the code again and enter the correct password")
            self.button_authenticate.button_style = 'danger'
        else:
            # Only works when running this file "main_dashboard" as jupyter notebook (.ipynb)
            #global spark
            #global ss
            spark = authentication
            ss = spark
            
            if not ss:
                return
            
            ##
            # resolve this better later !!
            IPython.display.clear_output()
            ##
            display(ss.sparkContext)
            self.button_authenticate.button_style = 'success'
            self.button_authenticate.description = 'Authenticated'
            return spark
        
    
    def observations(self):
        self.dropdown_dashboardSelect.observe(self.update_dashboard, 'value')
        self.button_authenticate.on_click(self.update_auth)
        
        
    
    def main_code(self):
        Layout = widgets.Layout
        self.comboBox()
        self.observations()
        combined_widgets = widgets.HBox([self.button_authenticate, self.dropdown_dashboardSelect])
        
        display(combined_widgets, self.out)
        




