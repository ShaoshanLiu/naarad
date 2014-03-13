# coding=utf-8
"""
© 2013 LinkedIn Corp. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""
import datetime
import logging
import os
import re
import sys
import threading
import json
import time
from datetime import date
from naarad.metrics.metric import Metric
import naarad.utils
import naarad.naarad_constants as CONSTANTS
from collections import defaultdict
logger = logging.getLogger('naarad.metrics.linkedin_mobile_web_metric')

class LinkedInMobileWebMetric(Metric):
  """ 
  Class for LinkedIn mobile web logs, deriving from class Metric 
  Note that this is for LinkedIn only
  """
  clock_format = '%Y-%m-%d %H:%M:%S'
  val_types = ('profileEdit_Actions', 'sendMessage_Actions', "share_Actions", 'updates_m_sim2_updates_PageViews', 'misc_m_sim2_login_PageViews', 'like_Actions', 'pymk_PageViews', 'jobs_PageViews', 'updates_PageViews', 'search_Actions', 'redirect_PageViews', 'profile_PageViews', 'inbox_PageViews', 'payments_PageViews', 'news_PageViews', 'subsSignup_Actions', 'invitation_Actions', 'groups_PageViews', 'comment_Actions', 'searchView_PageViews', 'all_PageViews', 'all_Actions', 'splash_m_sim2_download_app_splash_PageViews', 'subscriptions_PageViews')

  def __init__ (self, metric_type, infile, hostname, outdir, resource_path, label, ts_start, ts_end, rule_strings,
                **other_options):
    Metric.__init__(self, metric_type, infile, hostname, outdir, resource_path, label, ts_start, ts_end, rule_strings)
    self.mobile_web_results = defaultdict(lambda: defaultdict(None))
    self.sub_metrics = self.val_types
    self.sub_metric_description = {
      "profileEdit_Actions" : "the number of profile edit actions",
      "sendMessage_Actions" : "the number of send message actions",
      "share_Actions" : "the number of share actions",
      "updates_m_sim2_updates_PageViews" : "the number of m_sim2_updates page views",
      "misc_m_sim2_login_PageViews" : "the number of m_sim2 login page views",
      "like_Actions" : "the number of like actions",
      "pymk_PageViews" : "the number of pymk page views",
      "jobs_PageViews" : "the number of jobs page views",
      "updates_PageViews" : "the number of updates page views",
      "search_Actions" : "the number of search actions",
      "redirect_PageViews" : "the number of redirect page views",
      "profile_PageViews" : "the number of profile page views",
      "inbox_PageViews" : "the number of inbox page views",
      "payments_PageViews" : "the number of payments page views",
      "news_PageViews" : "the number of news page views",
      "subsSignup_Actions" : "the number of subscription signup actions",
      "invitation_Actions" : "the number of invitation actions",
      "groups_PageViews" : "the number of groups page views",
      "comment_Actions" : "the number of comment actions",
      "searchView_PageViews" : "the number of SearchView page views",
      "all_PageViews" : "the number of all page views",
      "all_Actions" : "the number of all actions", 
      "splash_m_sim2_download_app_splash_PageViews" : "the number of splash m_sim2 download app splash page views",
      "subscriptions_PageViews" : "the number of subscriptions page views"
    }

  def extract_mobile_web_results(self):
    """
    extract mobile web results from the input file
    """
    with open(self.infile, 'r') as inf:
      for line in inf:     
        data = line.split(',')
        if (not data[0]) or (not data[1]) or (not data[2]):
          continue 
        if "phone-fe phone-web profileEdit Actions WoW" in data[2]:
          self.mobile_web_results["profileEdit_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web sendMessage Actions WoW" in data[2]:
          self.mobile_web_results["sendMessage_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web share Actions WoW" in data[2]:
          self.mobile_web_results["share_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web updates m_sim2_updates PageViews WoW" in data[2]:
          self.mobile_web_results["updates_m_sim2_updates_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web misc m_sim2_login PageViews WoW" in data[2]:
          self.mobile_web_results["misc_m_sim2_login_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web like Actions WoW" in data[2]:
          self.mobile_web_results["like_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web pymk  PageViews WoW" in data[2]:
          self.mobile_web_results["pymk_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web jobs  PageViews WoW" in data[2]:
          self.mobile_web_results["jobs_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web updates  PageViews WoW" in data[2]:
          self.mobile_web_results["updates_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web search Actions WoW" in data[2]:
          self.mobile_web_results["search_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web redirect  PageViews WoW" in data[2]:
          self.mobile_web_results["redirect_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web profile  PageViews WoW" in data[2]:
          self.mobile_web_results["profile_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web inbox  PageViews WoW" in data[2]:
          self.mobile_web_results["inbox_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web payments  PageViews WoW" in data[2]:
          self.mobile_web_results["payments_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web news  PageViews WoW" in data[2]:
          self.mobile_web_results["news_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web subsSignup Actions WoW" in data[2]:
          self.mobile_web_results["subsSignup_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web invitation Actions WoW" in data[2]:
          self.mobile_web_results["invitation_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web groups  PageViews WoW" in data[2]:
          self.mobile_web_results["groups_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web comment Actions WoW" in data[2]:
          self.mobile_web_results["comment_Actions"].update({data[0]:data[1]})
        elif "phone-fe phone-web searchView  PageViews WoW" in data[2]:
          self.mobile_web_results["searchView_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web all  PageViews WoW" in data[2]:
          self.mobile_web_results["all_PageViews"].update({data[0]:data[1]})
        elif "phone-fe phone-web all Actions WoW" in data[2]:
          self.mobile_web_results["all_Actions"].update({data[0]:data[1]}) 
        elif "phone-fe phone-web splash m_sim2_download_app_splash PageViews WoW" in data[2]:
          self.mobile_web_results["splash_m_sim2_download_app_splash_PageViews"].update({data[0]:data[1]}) 
        elif "phone-fe phone-web subscriptions  PageViews WoW" in data[2]:
          self.mobile_web_results["subscriptions_PageViews"].update({data[0]:data[1]}) 
 
  def generate_files_for_metric(self, metric):
    """
    generate analysis results files for the current metric
    """ 
    if metric in self.mobile_web_results:
      ts = None
      output_file = self.get_csv(metric)
      with open(output_file, 'w') as outf:
        for ts in sorted(self.mobile_web_results[metric].iterkeys()):
          outf.write( naarad.utils.get_standardized_timestamp(ts, 'epoch') + ',' + self.mobile_web_results[metric][ts] + '\n' )
      self.csv_files.append(output_file)

  def generate_analysis_results(self):
    """
    generate analysis results files
    """
    # check if outdir exists, if not, create it
    if not os.path.isdir(self.outdir):
      os.makedirs(self.outdir)
    if not os.path.isdir(self.resource_directory):
      os.makedirs(self.resource_directory)
    # generate output files for each sub metric
    for key in self.sub_metric_description:
      self.generate_files_for_metric(key)
   
  def parse(self):
    """
    parse the mobile web logs
    """
    # extract mobile web results and store them in mobile_web_results
    self.extract_mobile_web_results()
    # generate analysis results files
    self.generate_analysis_results()
    return True
