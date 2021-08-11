def scrape_titles(_id: int, url: str,
                  export_stage_dir: str):
    """
        Main function organizing webscrapping of
        business categories from direct 118.

        Arguments:
        * url (str): URL to scrape from
    """
    import pandas as pd
    import logging
    import cloudscraper
    from bs4 import BeautifulSoup
    from pathlib import Path

    def scrape_url(url: str):
        """
            Scrapes bussines title from URL

            Arguments:
            * url (str): URL address of source to scrape

            Returns:
            * (pd.DataFrame): List business categories
        """

        class_element = "popTermsList"

        logging.info(f"Start processing url: {url}")
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            categories = soup \
                .find_all("ul", class_=class_element)[0] \
                .find_all("a")

            df = pd.DataFrame({
                'url': url,
                'business_category': list(map(lambda x: x.contents,
                                              categories)),
                'href': list(map(lambda x: x.attrs['href'], categories)),
            })
            df = df.explode('business_category').reset_index(drop=True)
            logging.info(f"Finish processing url: {url}")
            return df
        else:
            logging.error(f"Failure processing url: {url}")
            logging.error(f"Failure with status code: {response.status_code}")
            logging.error(f"Failure with text: {response.text}")

    df = scrape_url(url)
    Path(export_stage_dir).mkdir(parents=True, exist_ok=True)
    fpath = f"{export_stage_dir}{_id}.csv.zip"
    df.to_csv(fpath, index=False, compression='zip')
    logging.info(f"Finish parsing URL: {url} under partition {_id}")
