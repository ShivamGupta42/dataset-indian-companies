import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import timedelta

# PRE-REQUISITE :
# a) lxml parser would need to be installed e.g. by <pip|pip3.9> install lxml
# b) The data is exported to the external csv after every page is parsed from AmbitionBox
#    to ensure no data loss in case of any error.
# c) In case, any error is encountered. Continue from the pageNumber you encountered the error
#    by adjusting page_number_start variable. (The data is always appended to the csv File

page_number_start = 380
csvFile = "dataset/List_of_companies_in_India.csv"

# Note
# Go To https://www.ambitionbox.com/list-of-companies website and find the numebr of companies they have data of
# e.g. 742.2K unique companies
# total number of webpages will be = 742.2K companies / 30 companies per page = 24739
total_number_of_webpages = 24739

start_time = time.time()
dataframe_final = pd.DataFrame()

for page in range(page_number_start, total_number_of_webpages + 1):
    print("scraping webpage number: {page} of {total}".format(page=page, total=total_number_of_webpages))
    loop_time = time.time()

    # set page url and header
    url = "https://www.ambitionbox.com/list-of-companies?page={}".format(page)
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"}

    # get page response from the website
    response = requests.get(url, headers=header)
    # time.sleep(0.1)

    # pass the page to BeautifulSoup
    soup = BeautifulSoup(response.text, 'lxml')

    # find all the company cards from the webpage
    company_cards = soup.find_all("div", class_="company-content-wrapper")

    # extract all the required data from each company card and store them in a list
    name = []
    rating = []
    reviews = []
    domain = []
    location = []
    years_old = []
    employee_strength = []
    tags = []
    about = []

    # scrap scrap scrap!
    for card in company_cards:
        # 1. name
        try:
            name.append(card.find("h2").text.strip())
        except:
            name.append(None)

        # 2. rating
        try:
            rating.append(card.find("p", class_="rating").text.strip())
        except:
            rating.append(None)

        # 3. reviews
        try:
            reviews.append(card.find("a", class_="review-count sbold-Labels").text.strip().replace(" Reviews", ""))
        except:
            reviews.append(None)

        # 4. domain, 5. location, 6. years old & 7. employee strength
        info_list = card.find_all("p", class_="infoEntity sbold-list-header")
        dom = None
        loc = None
        old = None
        emp = None
        for i in range(4):
            try:
                if info_list[i].findChildren("i")[0]["class"][0] == 'icon-domain':
                    dom = info_list[i].text.strip()

                if info_list[i].findChildren("i")[0]["class"][0] == 'icon-pin-drop':
                    loc = info_list[i].text.strip()

                if info_list[i].findChildren("i")[0]["class"][0] == 'icon-access-time':
                    old = info_list[i].text.strip()

                if info_list[i].findChildren("i")[0]["class"][0] == 'icon-supervisor-account':
                    emp = info_list[i].text.strip()
            except:
                pass

        domain.append(dom)
        location.append(loc)
        years_old.append(old)
        employee_strength.append(emp)

        # # 8. tags
        # t = []
        # try:
        #     for tag in card.find_all("a", class_="ab_chip"):
        #         t.append(tag.text.strip())
        #     t = ', '.join(t)
        #     tags.append(t)
        # except:
        #     tags.append(None)
        #
        # # 9. about
        # try:
        #     about.append(card.find("p", class_="description").text.strip())
        # except:
        #     about.append(None)

    # make a dictionary containing all the data extracted
    col_dic = {
        "name": name,
        "rating": rating,
        "reviews": reviews,
        "domain": domain,
        "location": location,
        "years_old": years_old,
        "employee_strength": employee_strength
    }

    # pass the dictionary to pandas to create a dataframe (page)
    df = pd.DataFrame(col_dic)

    # append the dataframe to the final dataframe (the whole website)
    dataframe_final = dataframe_final.append(df, ignore_index=True)

    # success
    print("success!")
    print("time taken:", round((time.time() - loop_time) * 1000, 2), "ms")
    print("total time elapsed:", str(timedelta(seconds=(time.time() - start_time))))
    print()

    # Export the data to external csv after every loop to ensure no data loss in case of any error
    # continue from the pageNumber you encountered error by adjusting page_number_start
    dataframe_final.to_csv(csvFile, encoding="utf-8", mode="a",
                           header=False)

end_time = time.time()
print("full website scraped successfully!")
print("total time taken:", str(timedelta(seconds=(end_time - start_time))))
print()

# Print some statistics about the final dataframe:
print("dataframe shape", dataframe_final.shape)
print()
print("column-wise null count")
print(dataframe_final.isna().sum())
print()
