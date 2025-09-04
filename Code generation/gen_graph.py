
from asyncio.windows_events import NULL
from collections import defaultdict
from distro import like
from networkx import null_graph
import pandas as pd
import numpy as np
import networkx

# Generate dictionnary -> key being the regions, values being the projects that have that region. 
# one dictionnary for countries with a subregion and other for countries without a subregion 
def create_dictionnary(df_pandas):
    # create a dictionnary
    Dict_sub_projects = defaultdict(set)
    Dict_no_sub_projects = defaultdict(set)

    # populate the dictionaries
    rows = df_pandas.iterrows()
    for index, row in rows: 
        sub_region = row['WVC_DPMS_SUB_REGIONID']
        project = row['PROJECT_ID']
        country = row['COUNTRY']
        # has a subregion 
        if pd.notna(sub_region):
            # dictionnary with key=sub_region and values = projects 
            Dict_sub_projects[sub_region].add(project)
        #doesn't have subregion
        else: 
            # dictionnary with key=countryID and values = projects 
            Dict_no_sub_projects[country].add(project)

    # test the dictionnary
    # print(Dict_projects)
    return Dict_sub_projects, Dict_no_sub_projects


# generate graph to make sure they overlap 
def create_graph(df_pandas, Dict_sub_projects): 
    # create the graph 
    graph = networkx.Graph()

    # grab the cities of every project
    pid_to_city = df_pandas.set_index('PROJECT_ID')['CITY'].to_dict()

    # every project that has any same region as a other project will be connected.
    for projects in Dict_sub_projects.values():
        # add nodes 
        graph.add_nodes_from(projects)
        # add edges
        for project_1 in projects:
            for project_2 in projects:
                # no need to edge with itself, so continue 
                if project_1 == project_2:
                    continue
                else:
                    city_1 = pid_to_city[project_1]
                    city_2 = pid_to_city[project_2]
                    # only edge if city 1 is null, city 2 is null or both cities are the same. so if same subregion , different cities -> don't overlap and no edge. 
                    if (pd.isna(city_1) or pd.isna(city_2) or (city_1 == city_2) ):
                        graph.add_edge(project_1, project_2)
    return graph 


# generate a list of projects with their overlap code 
def generate_OverCode(graph, df_country_id, df_pandas, projects_no_subregion):
    
    project_code_overlap = {}

    # generate a list of lists of projects that will share a overlap code 
    project_lists = list(networkx.connected_components(graph))
    country_counter = defaultdict(int)
    # map every project to it's country ID: key = projectID value : countryID
    project_to_countryID = df_pandas.set_index('PROJECT_ID')['COUNTRY'].to_dict()

    # map every country ID to it's country code: key = country id value : country code
    countryID_to_country = df_country_id.set_index('COUNTRY_HKEY')['COUNTRY_CODE'].to_dict()


    # generate overlap codes for every group of projects that overlap
    for lst in project_lists:
        # figure out country of the group of projects
        random_project = next(iter(lst))
        country_ID = project_to_countryID.get(random_project, "UNKNOWN")
        country = countryID_to_country.get(country_ID, "UNKNOWN")
        country_counter[country] += 1
        # the overlap code
        overlap_code = f"OV_{country}_{country_counter[country]}"
        for project in lst:
            project_code_overlap[project]= overlap_code

    # generate overlap codes for projects that lack a subregion 
    # they will be considered that country_0 for overlap codes. 
    for countryID, projects in projects_no_subregion.items():
        # if country is null, it's unknown
        if not countryID:
            countryID = "UNKNOWN"
        # get the country code using the country ID
        country_code = countryID_to_country.get(countryID, "UNKNOWN")
        if (country_code is None):
            country_code = "UNKNOWN"
        # create the overlap
        country_code = country_code.replace(" ", "_")
        overlap_code = f"OV_{country_code}_0"
        for project in projects:
            project_code_overlap[project] = overlap_code

    return project_code_overlap