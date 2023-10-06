from playwright.sync_api import sync_playwright
import csv
import json
import requests
import pdfkit
import os
#  link dowload https://wkhtmltopdf.org/downloads.html
config = pdfkit.configuration()
email = "hr@ncc.asia"
password = "nccasia200896"
# Parent_dir
parent_dir = '/opt/airflow/dags/crawl_post_cv/'
# Crawl cv
def crawl_cv(target:str):
    parsed = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(0)
        page.goto("https://tuyendung.topcv.vn/app/search-cv?url_redirect=%2Fjobs")
        page.get_by_placeholder("Email").click()
        page.get_by_placeholder("Email").fill(email)
        page.get_by_placeholder("Mật khẩu").click()
        page.get_by_placeholder("Mật khẩu").click()
        page.get_by_placeholder("Mật khẩu").fill(password)
        page.get_by_role("button", name="Đăng nhập").click()
        page.wait_for_selector("div.container-fluid.page-content")
        page.wait_for_selector("div.shadow-sm.border.rounded")
        page.wait_for_selector("div.box-campaigns.px-4")
        page.wait_for_selector("div.shadow-sm.border.rounded div.box-campaigns.px-4")
        se = page.query_selector_all('div.shadow-sm.border.rounded div.box-campaigns.px-4 ')
        al = se[0].query_selector_all('div.border.rounded.p-4.d-flex.align-items-center.mb-3')
        for idx, x in enumerate(al):
            if x.query_selector("p").inner_text() == target:
                print(x.query_selector("p").inner_text())
                page.get_by_role("link", name="Tìm CV").nth( idx + 1).click()
        page.get_by_text("Tất cả nội dung").click()
        page.wait_for_selector('tbody.ta-impression-container')
        page_num = 2
        active = 1
        link ='https://tuyendung.topcv.vn/app/recruitment-campaigns/1072515/search-cv?page=' + str(active) 
        while  active <= page_num :
            active+=1
            link ='https://tuyendung.topcv.vn/app/recruitment-campaigns/1072515/search-cv?page=' + str(active) 
            page.set_default_timeout(0)
            page.goto(link)
            page.wait_for_selector('tbody.ta-impression-container')
            page.wait_for_selector('div.d-flex.justify-content-center nav ul.pagination')
            employees = page.query_selector_all('tr')
            for em in employees:
                page.wait_for_selector('div')
                viewmore =  page.query_selector_all('div.text-primary.show-more.mt-1')
                fullname = em.query_selector('a.cv-fullname')
                position = em.query_selector_all('div.ml-1 div.d-flex.align-items-start.mt-1.mt-2')
                detail = em.query_selector_all('td.align-top')
                obj = {'id':'','fullname':'','position':'','place':'','experiment':'','educated':'','target':'','linkcv':''}
                obj['id']= em.get_attribute('data-ta-cv_id')
                if len(detail)>0:
                    # show all experiment
                        for x in viewmore:
                                page.get_by_text("Xem thêm...").first.click()
                        obj['fullname'] = str(fullname.inner_text())
                        if len(position) >= 1:
                            for pos in position:
                                if pos.query_selector('div').inner_html() =='<i class="far fa-briefcase"></i>':
                                    obj['position'] = str(pos.query_selector('span').inner_text())
                                elif pos.query_selector('div').inner_html() =='<i class="far fa-map-marker"></i>':
                                    obj['place'] = str(pos.query_selector('span').inner_text())
                        mbs = detail[1].query_selector_all('div.mb-3')
                        for mb in mbs:
                            title = mb.query_selector('strong').inner_text()
                            if 'Kinh nghiệm' in str(title):
                                text = ''
                                exp = mb.query_selector_all('div.d-flex.align-items-start.mt-1')
                                for info in exp:
                                    text +=' '+ info.query_selector('span').inner_text()
                                obj['experiment']=str(text)
                            elif 'Học vấn' in str(title) :
                                text = ''
                                exp = mb.query_selector_all('div.d-flex.align-items-start.mt-1')
                                for info in exp:
                                    text +=' '+ info.query_selector('span').inner_text()
                                obj['educated']=str(text)
                            elif 'Mục tiêu sự nghiệp' in str(title) :
                                obj['target']= str(mb.query_selector('p.mb-0').inner_text())
                linktitle = em.query_selector_all('a.font-weight-bold.text-primary-hover.text-capitalize.cv-fullname')
                if len(linktitle)==1 and em.get_attribute('data-ta-cv_id') is not None:
                    linkcv=''
                    for x in linktitle:
                        linkcv = x.get_attribute('href')
                    page2 = context.new_page()
                    page2.goto(linkcv)
                    # Get link cv
                    def handle(response):
                        if '/api/v1/recruitment-campaigns/1072515/search-cv/cvs' in response.url:
                            obj['linkcv'] = response.json()["cv"]["preview_cv_url"]
                    page2.on("response", handle) 
                    page2.wait_for_selector('div.primary-col')
                    page2.wait_for_selector('iframe')
                    page2.wait_for_selector('div.secondary-col')
                    page2.close()
                parsed.append(obj)
                for ele in parsed:
                    if ele['id'] is None:
                        parsed.remove(ele)
                print(obj)
    csv_output(parsed)
# download CV
def download_cv_csv(file:str):
    if(os.path.exists(parent_dir + '/store') == False):
        os.mkdir(parent_dir + '/store')
    with open(file,"r",encoding='utf-8') as f:
           der = csv.reader(f)
           rows = []
           for row in der:
                if row:
                    rows.append(row)
           for row in rows:
             if len(row[7])>40:
                r = requests.get(row[7])
                path= parent_dir + '/store/'+str(row[0])+'.pdf'
                content_type = r.headers.get('content-type')
                if 'application/pdf' in content_type:
                    pdf = open(path, 'wb')
                    pdf.write(r.content)
                    pdf.close()                
                else:
                    try:
                       pdfkit.from_url(row[7], output_path=path,configuration=config)
                    except:
                       print("empty")
# JSON output
def json_output(arr:list):
    with open(parent_dir + "cvs.json", "a", encoding='utf-8') as outfile:
        outfile.write('[')
        for x in range(len(arr)):
                json_object = json.dumps(arr[x], indent=4,ensure_ascii=False)
                outfile.write(json_object)
                if x < len(arr)- 1:
                   outfile.write(",")
        outfile.write(']')
# CSV output
def csv_output(arr:list):
    field_names = ['id','fullname','position','place','experiment','educated','target','linkcv']
    # file csv cv to dowload
    with open(parent_dir + 'cv.csv', 'w+', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
        writer.writerows(arr)
    # save cv
    with open(parent_dir + 'cvs.csv', 'a', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = field_names)
        writer.writeheader()
        writer.writerows(arr)
    
if __name__ == "__main__":
    crawl_cv('Tuyển DEVELOPER')
    download_cv_csv(parent_dir + 'cv.csv')
    # readfilecsv('cv.csv')






