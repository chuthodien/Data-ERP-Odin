from playwright.sync_api import  sync_playwright
import json
import os
import requests
from datetime import datetime
from functools import reduce
time = datetime.now().strftime("%y-%m-%d %H:%M:%S")
API_LINK = 'http://talent-api.nccsoft.vn/api/services/app/Public/GetPostPaging'
API_KEY='tkmciyxeojqd'
API_ENDPOINT = 'http://talent-api.nccsoft.vn/api/services/app/Public/UpdatePostsMetadata'
# email or phonenumber to login facebook:
account=''
# password facebook
password =''
headers =  {
    "X-Secret-Key":API_KEY
    }
def json_output(arr:list):
        with open('stats.json', 'w+', encoding='utf-8') as outfile:
            stats = {
                "stats":arr
            }
            json.dump(stats,outfile,indent=4,ensure_ascii=False)
def crawl_post(arr:list):
    '''Get url to crawl post'''
    parsed=[]
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(0)
        page.goto("https://www.facebook.com/")
        page.get_by_test_id("royal_email").click()
        page.get_by_test_id("royal_email").fill(account)
        page.get_by_test_id("royal_pass").click()
        page.get_by_test_id("royal_pass").fill(password)
        page.get_by_test_id("royal_login_button").click()
        page.get_by_role("link", name="Notifications").click()
        page.get_by_role("link", name="Notifications").click()
        # print(arr)
        def getinteract(page):
                like, comment, share = (0,0,0)
                if page.query_selector('div.x6s0dn4.x78zum5.x1iyjqo2.x6ikm8r.x10wlt62 span.xt0b8zv.x1jx94hy.xrbpyxo.xl423tq span span.x1e558r4') is not None:
                    like = page.query_selector('div.x6s0dn4.x78zum5.x1iyjqo2.x6ikm8r.x10wlt62 span.xt0b8zv.x1jx94hy.xrbpyxo.xl423tq span span.x1e558r4').inner_html()
                if page.query_selector('''div.x9f619.x1n2onr6.x1ja2u2z.x78zum5.x2lah0s.x1qughib.x1qjc9v5.xozqiw3
                        .x1q0g3np.xykv574.xbmpl8g.x4cne27.xifccgj''') is not None  :
                    to = page.query_selector('''div.x9f619.x1n2onr6.x1ja2u2z.x78zum5.x2lah0s.x1qughib.x1qjc9v5.xozqiw3
                        .x1q0g3np.xykv574.xbmpl8g.x4cne27.xifccgj''').query_selector_all('div.x9f619.x1n2onr6.x1yrsyyn')
                    comment = to[1].query_selector("span").inner_text()
                if len(page.query_selector_all("div.x9f619.x1n2onr6.x1ja2u2z.x78zum5.x2lah0s.x1qughib.x6s0dn4.xozqiw3.x1q0g3np.xwrv7xz.x8182xy.x4cne27.xifccgj span"))>1 :
                    share = page.query_selector_all("div.x9f619.x1n2onr6.x1ja2u2z.x78zum5.x2lah0s.x1qughib.x6s0dn4.xozqiw3.x1q0g3np.xwrv7xz.x8182xy.x4cne27.xifccgj span")[1].inner_html()
                return like, comment, share
        for el in arr:
            # print(el)
            page2 = context.new_page()
            page2.goto(el['url'])
            page2.get_by_role("link", name="Notifications").click()
            page2.get_by_role("link", name="Notifications").click()
            content = ''
        
            if page2.query_selector('div.x3dsekl.x1uuop16') is not None:
                # print('DIEU KIEN 1')
                page2.wait_for_selector('div.x3dsekl.x1uuop16')
                tt = page2.query_selector("div.xyinxu5.x4uap5.x1g2khh7.xkhd6sd")
                if page2.query_selector('''div.x1i10hfl.xjbqb8w.x6umtig.x1b1mbwd.xaqea5y.xav7gou.x9f619.x1ypdohk.xt0psk2.xe8uvvx.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x16tdsg8.x1hl2dhg.xggy1nq.x1a2a7pz.xt0b8zv.xzsf02u.x1s688f'''):
                    page2.get_by_text("See more").first.click()
                texts = tt.query_selector_all('span div.x1e56ztr span')
                arr = []
                for x in texts:
                    arr.append(x.inner_text())
                s =sorted(set(arr),key=arr.index)
                for x in s:
                    content += x + '\n'
                if content =='':
                    content = tt.query_selector('span').inner_text()
                print(content)
                page2.wait_for_selector('div.x1jx94hy ul')
                like, comment, share = getinteract(page2)
                print(like,comment,share)
                ob = {"id":el['id'],"like":like,"comment":comment,"share":share,"content":content,"dateat":time}
                
                if page2.query_selector('div.x6s0dn4.x78zum5.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xe0p6wg') is not None:
                    page2.get_by_text("Top comments").first.click()
                    page2.get_by_text("All comments").first.click()
                print(ob)
                parsed.append(ob)
            
            elif page2.query_selector('div.x1iorvi4.x1pi30zi.x1l90r2v.x1swvt13') is not None:
                # print('DIEU KIEN 2')
                if page2.query_selector('''div.x1i10hfl.xjbqb8w.x6umtig.x1b1mbwd.xaqea5y.xav7gou.x9f619.x1ypdohk.xt0psk2.xe8uvvx.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x16tdsg8.x1hl2dhg.xggy1nq.x1a2a7pz.xt0b8zv.xzsf02u.x1s688f'''):
                    page2.get_by_text("See more").first.click()
                page2.wait_for_selector('div.x1iorvi4.x1pi30zi.x1l90r2v.x1swvt13')
                tt= page2.query_selector('''div.x1iorvi4.x1pi30zi.x1l90r2v.x1swvt13''')
                messages = tt.query_selector_all('.x1e56ztr span')
                if tt.query_selector('h4') is not None:
                    content +=tt.query_selector('h4 span').inner_text()
                for mes in messages:
                    content += mes.inner_text() + '\n'
                print(content)
                like, comment, share = getinteract(page2)
                print(like,comment,share)
                ob = {"id":el['id'],"like":like,"comment":comment,"share":share,"content":content,"dateat":time}
                print(ob)
                parsed.append(ob)
            elif page2.query_selector('div.x9f619.x1ja2u2z.x1xzczws.x7wzq59.x2bj2ny.xsag5q8.x1h0ha7o.xz9dl7a') is not None:
                # print('DIEU KIEN 3')
                if page2.query_selector('''div.x1i10hfl.xjbqb8w.x6umtig.x1b1mbwd.xaqea5y.xav7gou.x9f619.x1ypdohk.xt0psk2.xe8uvvx.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x16tdsg8.x1hl2dhg.xggy1nq.x1a2a7pz.xt0b8zv.xzsf02u.x1s688f'''):
                    page2.get_by_text("See more").first.click()
                page2.wait_for_selector('div.x1yztbdb.x1n2onr6.xh8yej3.x1ja2u2z')
                fed = page2.query_selector_all('div[role=feed]')
                content=fed[0].query_selector("div.x1yztbdb.x1n2onr6.xh8yej3.x1ja2u2z div.x1iorvi4.x1pi30zi.x1l90r2v.x1swvt13 span").inner_text()
                ob = {"id":el['id'],"like":like,"comment":comment,"share":share,"content":content,"dateat":time}
                print(ob)
            else:
                page2.close()
            page2.close()
    json_output(parsed)

def putPost(file:str):
	with open(file,'r',encoding='utf-8') as out:
		cvs = json.load(out)
		for item in cvs['stats']:
			post = {
				"Id":item['id'],
				"Metadata":json.dumps(item)
				}
			r = requests.put(url=API_ENDPOINT,data=post,headers=headers)
			pastebin_url = r.text
			print("The pastebin URL is:%s"%pastebin_url)
def getPost():
    parsed = []
    r = requests.get(url=API_LINK,headers=headers)
    pastebin_url = r.json()
    list_item  =pastebin_url['result']['result']['items']
    for item in list_item:
        # print({"id":item['id'],"url":item['url']})
        parsed.append({"id":item['id'],"url":item['url']})
    return parsed
if __name__ == "__main__":
	crawl_post(getPost())
	putPost("stats.json")
	
    
    
    
    
    






