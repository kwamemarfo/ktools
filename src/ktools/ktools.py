#!/usr/bin/env python
# coding: utf-8

from ktools.modules import kt


def get_latest_parquet(parquet_name):
    from ktools.modules import get_latest_parq
    get_latest_parq.get_latest_parquet(parquet_name)
        

def dashboards():
    from ktools.modules import main_dashboard
    main_dashboard.Dashboards().main_code()
    
        

