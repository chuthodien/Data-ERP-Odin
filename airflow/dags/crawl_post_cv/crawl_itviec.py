from playwright.sync_api import sync_playwright
import csv
from datetime import datetime
time = datetime.now().strftime("%y-%m-%d %H:%M:%S")
email=''
password = ''
job_fields=['title','skills','address','salary','type_work','date_post','company_name','company_text','type_company','member_company','day_work','country_company','ot','reason_choice','job_description','skills_and_experience','detail_benefit']
def crawl_itviec():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(0)
        page.goto("https://itviec.com/viec-lam-it")
        page.locator("#navbarNavDropdown").get_by_role("link", name="Đăng Nhập").click()
        page.get_by_placeholder("Email").click()
        page.get_by_placeholder("Email").fill("6051071029@st.utc2.edu.vn")
        page.get_by_placeholder("Password").click()
        page.get_by_placeholder("Password").fill("TuanDat160117@")
        page.get_by_role("button", name="Sign in with Email").click()
        page.get_by_role("button", name="×").click()
        page.locator("#navbarNavDropdown").get_by_role("link", name="All Jobs").click()
        active=0
        end=10
        while active<end:
            page.wait_for_selector('.search_jobs')
            page.wait_for_selector('.first-group')
            jobs = page.query_selector_all('.first-group .job')
            i=1
            
            detail_list =[]
            for job in jobs:
                if i==1:
                    page.locator(".job_content").first.click()
                    page.wait_for_selector('.search-page__job-preview')
                    page.wait_for_selector('.job-details')
                else:
                    page.locator(f"div:nth-child({i}) > .job_content").click()
                    page.wait_for_selector('.search-page__job-preview')
                    page.wait_for_selector('.job-details')
                
                page_detail = page.query_selector('.search-page__job-preview')
                overview = page_detail.query_selector('.job-details__overview')
                job_header = page_detail.query_selector('.job-details__header')
                company = page_detail.query_selector('.search-page-employer-overview')
                company_name = company.query_selector('.search-page-employer-overview__name').inner_text()
                company_text = company.query_selector('.search-page-employer-overview__headline-text').inner_text()
                # overview

                job_detail={}
                
                job_detail['title'] = job_header.query_selector('h1.job-details__title').inner_text()
                job_detail['skills'] =[]
                for sk in overview.query_selector_all('.job-details__tag-list a'):
                    job_detail['skills'].append(sk.inner_text())

                more_info = ['salary','address','type_work','date_post']
                svg_item = overview.query_selector_all('.svg-icon')

                e=-1
                job_detail['address']=''
                for  sv in svg_item:

                    if '<use xlink:href="#location_icon"></use>' in str(sv.query_selector('.svg-icon__icon').inner_html()):
                        job_detail['address'] += sv.query_selector('.svg-icon__text span').inner_text() + '\n'
                        e=1
                    else:
                        e+=1
                        job_detail[more_info[e]]=sv.query_selector('.svg-icon__text').inner_text()
                # something about company 
                items = page_detail.query_selector_all('.search-page-employer-overview__characteristics .svg-icon__text')
                job_detail['company_name'] = company_name
                job_detail['company_text'] = company_text
                keyob =['type_company','member_company','day_work','country_company','ot']
                for idx, item in enumerate(items):
                   job_detail[keyob[idx]]=item.inner_text()
                # -------------

                if page_detail.query_selector('.job-details__top-reason-to-join-us ul') is not None:
                    reason_text =''
                    reasons = page_detail.query_selector_all('.job-details__top-reason-to-join-us ul li')
                    for re in reasons:
                        reason_text +=re.inner_text()
                    job_detail['reason_choice']= reason_text + '\n'
                job_detail_pa = page_detail.query_selector_all('.job-details__paragraph')
                job_detail['job_description'] = job_detail_pa[0].inner_text()
                job_detail['skills_and_experience']= job_detail_pa[1].inner_text()
                job_detail['detail_benefit']= job_detail_pa[2].inner_text()
            
                detail_list.append(job_detail)
                i += 1
                print(job_detail)
            active+=1
            print(len(detail_list))
            page.get_by_role("link", name=">").click()
            csv_output(detail_list, job_fields, '/opt/airflow/dags/crawl_post_cv/job_itviec.csv','/opt/airflow/dags/crawl_post_cv/job_itviec_history.csv')

def csv_output(arr_ob:list,arr_key:list,file_name:str,file_name_history:str):
    '''
    arr_ob: list object
    arr_key: get list column
    file_name:job_itviec.csv,'st.csv'.... save file crawl and post to API
    file_name_history:'job_itviec.csv','st.csv'.... stored all file
    '''
    try:
        print('Start writing to file ' + file_name)
        with open(file_name, 'w+', encoding='utf-8', newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = arr_key)
            writer.writeheader()
            writer.writerows(arr_ob)
    except:
        print('Error writing to file ' + file_name)

if __name__ == "__main__":
    crawl_itviec()
