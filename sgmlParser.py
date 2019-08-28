
from html.parser import HTMLParser
import unicodedata,re,os
from bs4 import BeautifulSoup,CData
from multiprocessing import Pool,Process,Queue, Lock,Value
from PIL import Image,ImageFile
from PIL import ImageSequence
from PIL import TiffImagePlugin
from jeraconv import jeraconv
import json,time,sys
import tagVariables as tv
from functools import partial
import numpy as np
# import xmltodict,time

class MyHTMLParser(HTMLParser):
    fullTagList = []
    fullTagListsub = []

    def __init__(self):
        HTMLParser.__init__(self)
        self.fullTagList = []
        self.fullTagListsub = []
    def handle_starttag(self, tag, attrs):
        mkTag = ''

        if attrs:
            for at in attrs:
                # print(at)
                mkTag += ' '+at[0]+'="'+at[1]+'"'
                # print(mkTag)

        self.fullTagList.append('<'+tag+mkTag+'>')

    def handle_endtag(self, tag):

        self.fullTagList.append('</'+tag+'>')

    def handle_data(self, data):
        self.fullTagList.append(data)

def cdFullToHalf(txt):
    reTxt = ''
    for i in txt:
        if re.findall('\d+',i):
            reTxt += unicodedata.normalize('NFKC',i)
        else:
            if i == '年' or i == '第' or i == '号':
                pass
            else:
                if i == '（' or i == '）':
                    reTxt += unicodedata.normalize('NFKC',i)
                else:
                    reTxt += i
    return reTxt

def fullToHalf(txt):
    reTxt = ''
    for i in txt:
        if re.findall('\d+',i):
            reTxt += unicodedata.normalize('NFKC',i)
        else:

            reTxt += i
    return reTxt

def dateParsing(originDate):

    splitDate = originDate.split('（')
    splitDateSize = len(splitDate)
    convertedDate = ''
    jpDate = re.search('.*年.*月.*日',originDate)
    j2w = jeraconv.J2W()
    if jpDate:
        jpYearSplit = jpDate.group().split('年')
        jpYear = j2w.convert(jpYearSplit[0]+'年')
        jpMonthDayNomal=unicodedata.normalize('NFKC',jpYearSplit[1])
        jpMonthSplit = jpMonthDayNomal.split('月')

        jpDaySplit = jpMonthSplit[1].split('日')
        convertedDate = str(jpYear)+jpMonthSplit[0].zfill(2)+jpDaySplit[0][0:2].zfill(2)

    else:
        raise Exception('날짜이상 ] FILE_ID > {0} , DATE > {1}'.format(originDate))


    # jpDate = re.search('.*年.*月.*日',originDate)
    # if splitDateSize  == 1:
    #
    #     j2w = jeraconv.J2W()
    #     if jpDate:
    #         jpYearSplit = jpDate.group().split('年')
    #         jpYear = j2w.convert(jpYearSplit[0]+'年')
    #         jpMonthDayNomal=unicodedata.normalize('NFKC',jpYearSplit[1])
    #         jpMonthSplit = jpMonthDayNomal.split('月')
    #         convertedDate = str(jpYear)+jpMonthSplit[0].zfill(2)+jpMonthSplit[1].zfill(2).replace('日','')
    #     else:
    #         dateSearch = re.search('\(.*',originDate)
    #         if dateSearch:
    #             nomalDate = unicodedata.normalize('NFKC',dateSearch.group())
    #             nomalDateSplit = nomalDate.split('.')
    #             nomalDateSplitSize = len(nomalDateSplit)
    #             if nomalDateSplitSize == 3:
    #                 convertedDate = ''.join(nomalDateSplit)
    #                 convertedDate = convertedDate.replace(')','').replace('(','')
    #             else:
    #                 print('날짜 이상 >',originDate)
    #         else:
    #             print('날짜 이상 >',originDate)
    # else:
    #     if splitDateSize == 2:
    #
    #         publicDate = unicodedata.normalize('NFKC',splitDate[1])
    #         publicDate = publicDate.replace(')','').replace(',','.').replace('・','.')
    #
    #         if publicDate[-1] == '.':
    #             publicDate = list(publicDate)
    #             del publicDate[-1]
    #             publicDate = ''.join(publicDate)
    #         splitPubDate = publicDate.split('.')
    #
    #         if len(splitPubDate) == 3:
    #             convertedDate = splitPubDate[0]+splitPubDate[1].zfill(2)+splitPubDate[2].zfill(2)
    #         else:
    #             print('날짜 이상 > ',originDate)
    return [convertedDate,jpDate]


def tagMake(fileInfo):
    # sys.stdout.write("\r Completed Percent > [ {0}% ] {1:0.2f}".format(str(int(fileIdx / totalFileNum*100+1)), (time.time()-startTime)/60 ))
    # sys.stdout.flush()
    # ['APC', '00', '1000001', '1000001', '1000001', 'C:/dev/1.legalJP/00.Data/JPJ_2000001/DOCUMENT/APC/1000001/1000001/1000001/1000001.SGM']
    global errCnt
    global totalCnt
    global counter
    global strTime
    try:
        if not 'AC' == fileInfo[0] or 'collection' == fileInfo[0]:
            fileNm = '_'.join(fileInfo[0:5])
            dirNm = '_'.join(fileInfo[0:2])
            contents = ''
            reContents = ''
            tifFileList = []
            rmTifFileList = []
            tifDict = {}
            # Data Variables

            #주문
            jumun = ''
            #결정
            concluPart = ''
            # 사건번호 , 심판번호
            inciNum = ''
            # Debug 사건번호 , 심판번호
            debugInciNum = ''
            # 판결 선고일
            courtDc = ''
            # 법원명
            courtNm = ''
            # 명칭
            invenNm = ''
            # 종류
            kindOfItem = ''
            #공보종별
            offGaze = ''

            finalDp = ''
            divMain = ''
            pbDate = ''
            publicDate = ''
            sysDate = ''
            dateCt = ''

            with counter.get_lock():
                counter.value += 1

            with open(fileInfo[5],'r',encoding='EUC-JP') as rf:
                for cont in rf:
                    contents += cont

            parser = MyHTMLParser()
            parser.feed(contents)

            # IMG Data
            if fileInfo[6]:
                if os.path.exists(fileInfo[6]):
                    tifFileList = imgParser(fileInfo[6],fileInfo[4])
                    rmTifFileList = list(set(tifFileList))
                    for tif in rmTifFileList:
                        imgNum = re.search('_\d+',tif)
                        imNum = int(imgNum.group().replace('_',''))+1
                        tifDict[str(imNum).zfill(6)] = tif

            # IMG Data END

            for fTag in parser.fullTagList:
                if '\n]>' == fTag or ']>' == fTag:
                    continue
                else:
                    reContents += fTag
            soup = BeautifulSoup(reContents,'html.parser')



            if 'CD' == fileInfo[0]:
                # 판결
                divMain = 'CD'

                #사건번호
                inciNumTxt = soup.find('litigation-number')
                if not inciNumTxt is None:
                    inciNum = cdFullToHalf(inciNumTxt.text.replace('\n',''))
                    debugInciNum = inciNumTxt.text.replace('\n','')

                 #판결선고일
                courtDcTxt = soup.find('court-decision-giving-date')

                if not courtDcTxt is None:
                    courtDc = fullToHalf(courtDcTxt.text.replace('\n',''))

                # 법원명
                courtNmTxt = soup.find('belonging')

                if not courtNmTxt is None:
                    courtNm = fullToHalf(courtNmTxt.text.replace('\n',''))

            else:
                # 심결
                divMain = 'AJ'

                # 심판번호
                inciNumTxt = soup.find('appeal-number')
                if not inciNumTxt is None:
                    inciNum = fullToHalf(inciNumTxt.text.replace('\n',''))
                    debugInciNum = inciNumTxt.text.replace('\n','')

                #판결선고일
                courtDcTxt = soup.find('appeal-decision-date')
                if not courtDcTxt is None:
                    courtDc = fullToHalf(courtDcTxt.text.replace('\n',''))

                # 법원명
                courtNmTxt = soup.find('publication-country')
                if not courtNmTxt is None:
                    courtNm =fullToHalf(courtNmTxt.text.replace('\n',''))
                #공보종별
                offiGazeTxt = soup.find('official-gazette-assortment')
                if not offiGazeTxt is None:
                    offGaze = offiGazeTxt.text.replace('\n','')

                finalDispTxt = soup.find('final-disposition')
                if not finalDispTxt is None:
                    finalDp = finalDispTxt.text.replace('\n','')



            for singleTag in soup.findAll():
                tagNm = singleTag.name

                if 'paragraph' == tagNm:
                    singleTag.string = CData(singleTag.text)

                if 'image' == tagNm:
                    if singleTag['file-id']:
                        singleTag['src'] = tifDict[singleTag['file-id']]
                        singleTag.name = 'img'
                        attrsList = []
                        for attr in singleTag.attrs:
                            if not 'src' == attr:
                                attrsList.append(attr)
                        for at in attrsList:
                            del singleTag[at]
                    else:
                        raise Exception('이미지 이상 ] FILE_ID > {0}'.format(fileNm))
                if 'sub-script' == tagNm or 'sup-script' == tagNm :
                    singleTag.decompose()

                if 'kind' == tagNm:
                    #종류
                    kindOfItem = singleTag.text.replace('\n','')

                if 'name-of-article' == tagNm or 'title-of-the-invention' == tagNm:
                    invenNm = singleTag.text.replace('\n','')

                if 'main-part' == tagNm:
                    jumun = singleTag.text

                if 'conclusion-part' == tagNm:
                    concluPart = singleTag.text



            divMain = '<DIV_MAIN>'+divMain+'</DIV_MAIN>'+'\n'
            divSub = '<DIV_SUB>'+fileInfo[0]+'</DIV_SUB>'+'\n'
            caseNum = '<CASENUMBER>'+inciNum+'</CASENUMBER>'+'\n'
            debugCaseNum = '<DEBUG_CASENUMBER>'+debugInciNum+'</DEBUG_CASENUMBER>' + '\n'
            caseNm = '<CASENAME>'+invenNm+'</CASENAME>'+'\n'
            senten = '<SENTENCE>'+jumun+'</SENTENCE>'+'\n'
            conClu = '<CONCLUSION>'+concluPart+'</CONCLUSION>'+'\n'
            fileIdTag = '<ID>'+fileNm+'</ID>'+'\n'
            kindTag = '<KIND>'+kindOfItem+'</KIND>'
            xmlPath = '<XML_PATH>'+'/data/prec_pdf/xml/'+dirNm+'/'+fileNm+'.xml'+'</XML_PATH>'+'\n'
            offGazeTag = '<OFFICIAL_GAZETTE>'+offGaze+'</OFFICIAL_GAZETTE>'
            finalDpTag = '<FINAL_DISPOSIT>'+finalDp+'</FINAL_DISPOSIT>'
            courtName = '<COURTNAME>'+courtNm+'</COURTNAME>'+'\n'

            if courtDc:
                dateCt = dateParsing(courtDc)
                if dateCt:
                    if dateCt[0]:
                        sysDate = '<SYSDATE>'+dateCt[0]+'</SYSDATE>'+'\n'
                    if dateCt[1]:
                        pbDate = dateCt[1].group()
            else:
                sysDate = '<SYSDATE></SYSDATE>'+'\n'

            if 'AJ' == divMain:
                publicDate = '<SENTENCEDATE></SENTENCEDATE>'+'\n'+'<CONCLUSIONDATE>'+pbDate+'</CONCLUSIONDATE>'+'\n'
            else:
                publicDate = '<SENTENCEDATE>'+pbDate+'</SENTENCEDATE>'+'\n' + '<CONCLUSIONDATE></CONCLUSIONDATE>'+'\n'

            allCont = str(soup.prettify(formatter="none")).replace('&minus;','-')

            templeteXML = '<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="/view/prec_jp.xsl"?>'+'\n'+'<root xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="bx_Dublin.xsd" >'\
            +'\n'+divMain + divSub + caseNum + kindTag + offGazeTag + finalDpTag + debugCaseNum + caseNm + senten + publicDate +conClu + fileIdTag +courtName+sysDate+xmlPath+allCont+'</root>'

            with open('C:/dev/4.pCourt/00.Data/'+dirNm+'/'+fileNm+'.xml','w',encoding='UTF-8') as pf:


                pf.write(templeteXML)
                clickerVal = counter.value % 2
                clicker = ''
                if clickerVal == 0:
                    clicker = '○'
                else:
                    clicker = '●'

                # sys.stdout.write("\r %s Completed Percent > [ %s % ] {2:0.2f} %02d:%02d:%02d" % (clicker,str(int(counter.value / totalCnt*100+1)),h, m, s))
                sys.stdout.write("\r {1} Completed Percent > [ {0}% ] {2:0.2f}".format(str(int(counter.value / totalCnt*100+1) ), clicker,( (time.time()-strTime)/60)) )
                sys.stdout.flush()

    except Exception as e:
        with errCnt.get_lock():
            errCnt.value += 1
        # sys.stdout.write("\r Err Count > [ {0} ] ".format(str(int(errCnt.value))))
        # sys.stdout.flush()

        with open('C:/dev/4.pCourt/err/judge/err.txt','a',encoding='UTF-8') as ef:
            errLog =fileNm+'>>'+ '_'.join(fileInfo[0:2])+'>>'+str(e)+'\n'
            ef.write(errLog)

        pass



def allFileList():
    totFileList = []
    imgFile = ''
    firstDepFolder = os.listdir(mainPath)
    procCnt = []
    for fdf in firstDepFolder:
        yn = re.sub('[^\d]','',fdf)[2:4] # 00,01,02..etc
        secondDepFolder = mainPath+'/'+fdf+'/DOCUMENT'
        for sdf in os.listdir(secondDepFolder):
            if sdf == 'AC':
                continue
            thDepFolder = secondDepFolder+'/'+sdf
            # print(sdf) # APC,APD....etc
            mkFolderNm = createMainPath+'/'+sdf+'_'+yn
            if not os.path.exists(mkFolderNm):
                os.mkdir(mkFolderNm+'/')

            for tdf in os.listdir(thDepFolder):

                if re.search('\d',tdf):
                    fourDepFolder = thDepFolder+'/'+tdf
                    for fodf in os.listdir(fourDepFolder):
                        chkCnt =int(len(totFileList)/385432*100)+1
                        if procCnt:
                            if not procCnt[-1] == chkCnt:
                                sys.stdout.write("\r File Path Parsing..[ {0}% ]".format(str(chkCnt)))
                                sys.stdout.flush()
                                procCnt.append(chkCnt)
                        else:
                            procCnt.append(chkCnt)

                        fivDepFolder = fourDepFolder+'/'+fodf
                        for fivdf in os.listdir(fivDepFolder):
                            sixDepFolder = fivDepFolder+'/'+fivdf

                            for fileLt in os.listdir(sixDepFolder):

                                if re.search('uni+',fileLt):
                                    pass
                                else:

                                    fileNm = fileLt.split('.')
                                    fileFllPath = sixDepFolder+'/'+fileNm[0]+'.SGM'
                                    if  re.search('.IMG',fileLt):
                                        imgPath = sixDepFolder+'/'+fileNm[0]+'.IMG'
                                        imgFile = imgPath

                                    totFileList.append( [sdf,yn,tdf,fivdf,fileNm[0],sixDepFolder+'/'+fileNm[0]+'.SGM',imgFile])

    return totFileList

def imgParser(imgPhLt,oriFileNm):
    im = Image.open(imgPhLt)
    imSize = im.n_frames
    tifList = []
    for imLen in range(0,imSize):
        imgNw = Image.open(imgPhLt)
        imgNw.seek(imLen)
        tifList.append('/data/prec_pdf/img/'+oriFileNm+'_'+str(imLen)+'.jpg')
        time.sleep(0.002)
    return tifList

counter = None
errCnt = None

def init(cnt,eCnt,tCnt,stTime):
    ''' store the counter for later use '''
    global counter
    global errCnt
    global totalCnt
    global strTime

    counter = cnt
    errCnt = eCnt
    totalCnt = tCnt
    strTime = stTime

if __name__ == '__main__':
    mainPath = 'C:/dev/1.legalJP/00.Data'
    createMainPath = 'C:/dev/4.pCourt/00.Data'
    tifFileList = []
    print("\n==========FILE PATH PARSING===============")
    fileInfoList = allFileList()
    print("\n==========FILE PATH COMPLETED============")
    print('\n============XML DATA PARSING==============')
    #
    # with  Pool(10) as pool:
    #     tifFileList.append(pool.map(imgParser,fileInfoList[1]))
    rmFileInfoList = list(set(map(tuple,fileInfoList)))
    counter = Value('i', 0)
    errCnt = Value('i', 0)
    #
    #
    totalFileNum = len(rmFileInfoList)
    startTime = time.time()
    # for fileIdx, fileInfo in enumerate(rmFileInfoList):
    #
    #     sys.stdout.write("\r Completed Percent > [ {0}% ] {1:0.2f}".format(str(int(fileIdx / totalFileNum*100+1)), (time.time()-startTime)/60 ))
    #     sys.stdout.flush()
    #     # if '1004133' == fileInfo[4]:
    #
    #     tagMake(fileInfo)
    #
    #

    with  Pool(10,initializer = init, initargs = (counter,errCnt,totalFileNum,startTime, ) ) as pool:
        pool.map(tagMake,rmFileInfoList)
        print( (time.time()-startTime)/60)
        print('\n========XML DATA PARSING COMPLETED==========')
        sys.stdout.write("\r Err Count > [ {0} ] ".format(str(int(errCnt.value))))


    # break



        # print(tagMake(parser))





        # print(soup.prettify(formatter="none"))
    #     print('============================================')
    #     print(tv.attrList)
        # with open('c:/dev/4.pCourt/test2.xml','w',encoding='UTF-8') as fs:
        #     fs.write(allCont)
