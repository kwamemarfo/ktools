#!/usr/bin/env python
# coding: utf-8

import ipywidgets as widgets
from IPython import get_ipython
from ktools.modules import kt


class Create_env():
    out = widgets.Output(layout={'border': '1px solid black'})
    
        
     ########################### WIDGETS ###########################################     
    def comboBox(self):
        self.text_enviro_name = widgets.Text(
        placeholder='Select name for environment',
        description='Name:', 
        disabled=False
        )
    
        self.button_generate = widgets.Button(
            description='Create Environment',
            button_style='warning',
            icon='eye',
            layout={'margin': '0 80px 0 0'}
        )
    ##############################################################################
    
    @out.capture(clear_output = True)
    def create_environment(self, *args):
        get_ipython().system('pip3.8 install --user ipykernel')
        self.out.clear_output()
        get_ipython().system('python3.8 -m ipykernel install --user --name=$self.text_enviro_name.value')
        
    
    def observations(self):
        self.button_generate.on_click(self.create_environment)        
        
    
    def main_code(self):
        Layout = widgets.Layout
        self.comboBox()
        self.observations()
        combined_widgets = widgets.HBox([self.text_enviro_name, self.button_generate])
        display(combined_widgets, self.out)


