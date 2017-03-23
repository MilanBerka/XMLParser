#from __future__ import print_function
import pip
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'httplib2'])
pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'google-api-python-client'])

import httplib2
import os
import io 
import zipfile 
import pandas as pd
import xml.etree.ElementTree as ET
from keboola import docker

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from apiclient.http import MediaIoBaseDownload

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

CFG = docker.Config()
PARAMS = CFG.get_parameters()
FOLDER_NAMES = PARAMS.get('folderNames')
# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = PARAMS.get('scope')
CLIENT_SECRET_FILE = PARAMS.get('cljson')
APPLICATION_NAME = 'Drive API Python Quickstart'

# MAIN PARSER CLASSES
class Node:
    """ 
    Building block for the XMLParser Class.
    """
    def __init__(self, element, parentNode=None):
        self.element = element
        self.parentNode = parentNode
        if self.parentNode:
            self.parentTag = parentNode.tag
        self.tag = element.tag
        self.value = element.text
        self.childrenElementList = element.getchildren()
        self.childrenNodes = []
        if not self.childrenElementList:
            self.isLeaf = True
        else:
            self.isLeaf = False
        if self.isLeaf:
            self.dataFrame = pd.DataFrame({'parentTag':[self.parentTag],'{}.{}'.format(self.parentTag,self.tag):self.value})
        else:
            self.dataFrame = None
    
    def __str__(self):
        return str(self.tag)
    
    def __repr__(self):
        return str(self.tag)
    
    def feedforwardInit(self, recursive=False, level=0, treeDict={}):
        """ 
        Create children Nodes if there are any according to the XML tree structure. 
        If `recursive` is true, than all `childrenNodes` in the tree will be initialized.
        
        """
        self.childrenNodes = [Node(childElement,self) for childElement in self.childrenElementList]
        if recursive:
            level += 1
            for childNode in self.childrenNodes:
                if level in treeDict:
                    pass
                else:
                    treeDict[level]=[]
                treeDict[level].append(childNode)                
                childNode.feedforwardInit(True,level,treeDict)
            return False
            
    def childMerge(self):
        """ 
        Merge DataFrames, carried by the children of the nodes, into one DataFrame, which 
        will be then stored in `self.DataFrame`.
        """ 
        if self.isLeaf:
            pass       
        else:
            nodeList = []
            banNodesList = []
            childrenNodesList = self.childrenNodes.copy()
            for node in childrenNodesList:
                if node in banNodesList:
                    pass
                else:
                    subnodeList = []
                    for subnode in self.childrenNodes:
                        if node.tag == subnode.tag:
                            subnodeList.append(subnode)
                            banNodesList.append(subnode)
                        else:
                            pass
                    if len(subnodeList) > 1:
                        nodeList.append(subnodeList)
                    else:
                        nodeList.append(subnodeList[0])
            if isinstance(nodeList[0],list):
                resultingDataFrame = pd.concat([df.dataFrame for df in nodeList[0]],ignore_index=True)
            else:
                resultingDataFrame = nodeList[0].dataFrame
            # forcycle the rest
            for item in nodeList[1:]:
                if isinstance(item,list):
                    resultingDataFrame = resultingDataFrame.merge(pd.concat([df.dataFrame for df in item],ignore_index=True),how='inner',on='parentTag')
                else:
                    resultingDataFrame = resultingDataFrame.merge(item.dataFrame,how='inner',on='parentTag')
            self.dataFrame = resultingDataFrame
            if self.parentNode:
                self.dataFrame['parentTag'] = self.parentTag

class XMLParser:
    """
    XML Parser for Liftago. Takes either `path_to_xml` string (mandatory, set to None if unusued) or `extTree` ElementTree
    object. Method `parseToDataFrame` returns flattened xml as pandas.DataFrame, which can be then exported to csv via *.to_csv method
    """
    def __init__(self,path_to_xml,extTree=None):
        if extTree:
            root = extTree
        else:
            tree = ET.parse(path_to_xml)
            root = tree.getroot()
        self.rootNode = Node(root)
        self.treeDict = {0:[self.rootNode]}
        self.xmlDataFrame = None
    
    def parseToDataFrame(self,returnDataFrame=True):
        self.rootNode.feedforwardInit(recursive=True,level=0,treeDict=self.treeDict)
        for i in reversed(range(len(self.treeDict))):
            for node in self.treeDict[i]:
                node.childMerge()
        self.xmlDataFrame = self.rootNode.dataFrame
        if returnDataFrame:
            return self.xmlDataFrame
        
def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def extract_xml2csv(readZipfile,finalDataFrame=None):
    for fileInZipfileName in readZipfile.namelist():
        if '.xml' in fileInZipfileName.lower():
            if ('-T' in fileInZipfileName.upper()) or ('-M' in fileInZipfileName.upper()):
                pass
            else:
                openedXml = readZipfile.open(fileInZipfileName).read()
                loadedXml = ET.fromstring(openedXml.decode())
                toBeParsed = XMLParser(None,loadedXml.find('merchants'))  
                parsedXmlDataFrame = toBeParsed.parseToDataFrame()
                if finalDataFrame:
                    finalDataFrame = pd.concat([finalDataFrame.copy(),parsedXmlDataFrame])
                else:
                    finalDataFrame = parsedXmlDataFrame.copy()
        else:
            pass              
def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    results = service.files().list(
        pageSize=10,fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print('{0} ({1})'.format(item['name'], item['id']))

if __name__ == '__main__':
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=http)

    results = service.files().list(q="mimeType = 'application/vnd.google-apps.folder' \
    and name = 'CSOB AM 2017' and trashed=false",
        pageSize=50,fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    if not items:
        print('No files found.')
    else:
        finalDataFrame = None
        desiredFileId = items[0]['id']
        driveZipFiles = service.files().list(q="'{}' in parents and trashed=false and (mimeType contains 'zip') ".format(desiredFileId),
        pageSize=1000,fields="nextPageToken, files(id, name)").execute()
        zipitems = driveZipFiles.get('files', [])
        if not zipitems:
            print('No files found.')
        print('Files:')
        for item in zipitems:
            pass
            request = service.files().get_media(fileId=item['id'])
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            readZipfile = zipfile.ZipFile(fh, "r")
            extract_xml2csv(readZipfile,finalDataFrame)
        
        finalDataFrame.to_csv('/data/out/tables/parsedBatch.csv',index=None) 
