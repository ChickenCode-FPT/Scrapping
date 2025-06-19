import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time
import random
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# Cấu hình MongoDB Atlas
MONGO_URI = "mongodb+srv://lumconon0911:SWD-SWD@cluster.slolqwf.mongodb.net/"
DATABASE_NAME = "topdev_jobs_db"
COLLECTION_NAME = "job_details"
BASE_URL = "https://vieclam24h.vn"


def has_all_classes(tag, class_string):
    if not tag or not tag.has_attr('class'):
        return False
    target_classes_list = class_string.split()
    return all(cls in tag['class'] for cls in target_classes_list)


def get_html_content(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải trang web '{url}': {e}")
        return None


def extract_detail_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    unique_detail_links = set()

    target_classes_listing_page = "grid grid-cols-1 gap-y-2 lg:gap-y-2.5"

    container = soup.find('div',
                          class_=lambda c: has_all_classes(BeautifulSoup(f'<div class="{c}"></div>', 'html.parser').div,
                                                           target_classes_listing_page) if c else False)

    if container:
        links = container.find_all('a', href=True)
        for link in links:
            href = link['href']

            if "page" in href.lower():
                # print(f"Bỏ qua liên kết phân trang: {href}")
                continue

            if href.startswith('http'):
                unique_detail_links.add(href)
            else:
                full_link = f"{BASE_URL}{href}"
                unique_detail_links.add(full_link)
    else:
        print(f"Không tìm thấy container với các class: '{target_classes_listing_page}'.")

    return unique_detail_links


def extract_section_content(soup_obj, heading_text):
    lower_heading_text = heading_text.lower()

    heading_tag = None
    for tag_name in ['h2', 'h3', 'span']:
        heading_tag = soup_obj.find(tag_name, string=lambda text: text and lower_heading_text in text.lower())
        if heading_tag:
            break

    if heading_tag:
        content_element = None
        specific_content_class = "jsx-5b2773f86d2f74b mb-2 text-14 break-words text-se-neutral-80 text-description"

        content_element = heading_tag.find_next_sibling('div', class_='prose')

        if not content_element:
            for tag_type in ['div', 'span']:
                dummy_tag_html = f'<{tag_type} class="">content</{tag_type}>'
                potential_content = heading_tag.find_next_sibling(tag_type, class_=lambda c: has_all_classes(
                    BeautifulSoup(dummy_tag_html, 'html.parser').find(tag_type),
                    specific_content_class) if c else False)
                if potential_content:
                    content_element = potential_content
                    break

        if not content_element:
            content_element = heading_tag.find_next_sibling(['div', 'ul', 'ol', 'p', 'span'])

        if content_element:
            raw_text = content_element.get_text(separator=' ', strip=False)
            cleaned_text = re.sub(r'\s+', ' ', raw_text).strip()
            return cleaned_text
        else:
            raw_heading_text = heading_tag.get_text(separator=' ', strip=False)
            cleaned_heading_text = re.sub(r'\s+', ' ', raw_heading_text).strip()
            if cleaned_heading_text.lower() == lower_heading_text:
                return "Không tìm thấy."
            else:
                return cleaned_heading_text

    return "Không tìm thấy."


def normalize_url(url):
    """
    Chuẩn hóa URL về một định dạng nhất quán.
    Xóa các đoạn (fragments), sắp xếp tham số truy vấn và xóa các dấu gạch chéo thừa.
    """
    parsed = urlparse(url)
    path = parsed.path
    query_params = parse_qs(parsed.query, keep_blank_values=True)
    sorted_query = urlencode(sorted(query_params.items()), doseq=True)
    if path.endswith('/') and len(path) > 1:
        path = path.rstrip('/')
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    normalized_url = urlunparse((scheme, netloc, path, parsed.params, sorted_query, ''))
    return normalized_url


def process_detail_link(detail_url, collection):
    normalized_detail_url = normalize_url(detail_url)
    print(f"\n--- Đang xử lý liên kết chi tiết: {normalized_detail_url} ---")
    detail_html = get_html_content(normalized_detail_url)

    if detail_html:
        detail_soup = BeautifulSoup(detail_html, 'html.parser')

        job_title = detail_soup.find('title').get_text(strip=True) if detail_soup.find(
            'title') else "Không có tiêu đề."

        responsibilities_content = extract_section_content(detail_soup, "Mô tả công việc")
        if responsibilities_content == "Không tìm thấy.":
            responsibilities_content = extract_section_content(detail_soup, "Responsibilities")

        requirements_content = extract_section_content(detail_soup, "Yêu cầu công việc")
        if requirements_content == "Không tìm thấy.":
            requirements_content = extract_section_content(detail_soup, "Requirements")

        benefits_content = extract_section_content(detail_soup, "Quyền lợi")
        if benefits_content == "Không tìm thấy.":
            benefits_content = extract_section_content(detail_soup, "phúc lợi dành cho bạn")
        if benefits_content == "Không tìm thấy.":
            benefits_content = extract_section_content(detail_soup, "Benefits")

        # Nếu không có nội dung chính, bỏ qua
        if (responsibilities_content == "Không tìm thấy." and
                requirements_content == "Không tìm thấy." and
                benefits_content == "Không tìm thấy."):
            print(
                f"⚠️ Bỏ qua lưu vào database cho '{job_title}' ({normalized_detail_url}). Không tìm thấy nội dung Mô tả công việc, Yêu cầu công việc, hoặc Quyền lợi.")
            print("-" * (len(normalized_detail_url) + 40))
            return

        # Dữ liệu công việc để lưu trữ
        job_data = {
            "url": normalized_detail_url,
            "title": job_title,
            "responsibilities": responsibilities_content,
            "requirements": requirements_content,
            "benefits": benefits_content,
            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # --- LOGIC KIỂM TRA TRÙNG LẶP MỚI: Nếu trùng title HOẶC responsibilities thì KHÔNG THÊM ---

        # 1. Kiểm tra xem có bản ghi nào trùng title không
        duplicate_by_title = collection.find_one({'title': job_data['title']})

        # 2. Kiểm tra xem có bản ghi nào trùng responsibilities không
        duplicate_by_responsibilities = collection.find_one({'responsibilities': job_data['responsibilities']})

        if duplicate_by_title or duplicate_by_responsibilities:
            # Nếu tìm thấy trùng lặp theo title HOẶC responsibilities
            duplicate_criteria = []
            if duplicate_by_title:
                duplicate_criteria.append("Tiêu đề")
            if duplicate_by_responsibilities:
                duplicate_criteria.append("Mô tả công việc")

            print(
                f"⚠️ Phát hiện trùng lặp theo {' HOẶC '.join(duplicate_criteria)} cho '{job_data['title']}' ({job_data['url']}).")
            print(f"   Bỏ qua việc thêm bản ghi này để tránh trùng lặp nội dung.")
            print("-" * (len(normalized_detail_url) + 40))
            return  # Dừng hàm, không thêm vào database
        else:
            # Nếu không có trùng lặp theo tiêu đề HOẶC mô tả công việc,
            # thì đây là một công việc mới, chúng ta sẽ thêm nó vào.
            try:
                # Vẫn sử dụng upsert dựa trên URL để nếu một công việc có cùng URL nhưng không có nội dung
                # được thu thập trước đó, nó sẽ được cập nhật.
                # Tuy nhiên, nếu URL là hoàn toàn mới VÀ nội dung không trùng lặp, nó sẽ được thêm mới.
                result = collection.update_one(
                    {'url': job_data['url']},  # Filter by URL to ensure URL uniqueness (if index is active)
                    {'$set': job_data},
                    upsert=True  # Chèn nếu không tìm thấy URL, hoặc cập nhật nếu tìm thấy
                )

                if result.upserted_id:
                    print(f"✅ Đã thêm mới dữ liệu cho '{job_data['title']}' vào MongoDB (ID: {result.upserted_id}).")
                elif result.modified_count > 0:
                    print(f"✅ Đã cập nhật dữ liệu cho '{job_data['title']}' trong MongoDB (dữ liệu URL đã tồn tại).")
                else:
                    print(f"ℹ️ Dữ liệu cho '{job_data['title']}' đã tồn tại và không có thay đổi.")

            except Exception as e:
                # Xử lý trường hợp URL trùng lặp nếu chỉ mục URL duy nhất được bật và bạn cố gắng chèn
                # một URL đã có mà update_one không xử lý được (ít khả năng xảy ra với upsert=True)
                if "E11000 duplicate key error" in str(e) and "url" in str(e):
                    print(f"ℹ️ Dữ liệu cho '{job_data['title']}' ({job_data['url']}) đã tồn tại theo URL.")
                else:
                    print(f"❌ Lỗi khi lưu dữ liệu vào MongoDB cho '{job_data['title']}': {e}")

        # --- KẾT THÚC LOGIC KIỂM TRA TRÙNG LẶP MỚI ---

        print("\n" + "=" * 50)
        print(f"DỮ LIỆU TRÍCH XUẤT TỪ TRANG CHI TIẾT ({normalized_detail_url}):")
        print("=" * 50 + "\n")
        print(f"Tiêu đề: {job_title}")
        print(f"1. Mô tả công việc: {responsibilities_content}")
        print(f"2. Yêu cầu công việc: {requirements_content}")
        print(f"3. Quyền lợi: {benefits_content}")
        print("\n" + "=" * 50)

    else:
        print(f"Không thể tải nội dung HTML của trang chi tiết: {normalized_detail_url}")
    print("-" * (len(normalized_detail_url) + 40))


if __name__ == "__main__":
    client = None
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        print(f"✅ Đã kết nối thành công tới MongoDB Atlas cluster.")
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Đang sử dụng database '{DATABASE_NAME}' và collection '{COLLECTION_NAME}'.")

        # Đảm bảo các chỉ mục được thiết lập
        # CHỈ ĐỂ LẠI CHỈ MỤC DUY NHẤT TRÊN 'url' để ngăn URL bị trùng lặp.
        # Chúng ta KHÔNG TẠO chỉ mục duy nhất trên 'title' HOẶC 'responsibilities'
        # vì logic "OR" được xử lý trong mã Python.
        try:
            collection.create_index("url", unique=True)
            print("✅ Đã đảm bảo chỉ mục duy nhất trên trường 'url'.")
        except Exception as e:
            print(f"ℹ️ Lỗi khi tạo chỉ mục duy nhất trên 'url' (có thể đã tồn tại): {e}")

        # # Nếu bạn đã tạo các chỉ mục duy nhất khác trên title/responsibilities trước đó,
        # # bạn NÊN xóa chúng để tránh xung đột với logic mới. Ví dụ:
        # try:
        #     collection.drop_index("title_1")
        #     collection.drop_index("responsibilities_1")
        #     collection.drop_index("title_1_responsibilities_1") # Nếu có chỉ mục tổng hợp
        #     print("ℹ️ Đã xóa các chỉ mục duy nhất cũ trên title/responsibilities nếu có.")
        # except Exception as e:
        #     pass # Bỏ qua nếu chỉ mục không tồn tại

        initial_listing_url = "https://vieclam24h.vn/viec-lam-it-phan-mem-o8.html?occupation_ids[]=8&sort_q=priority_max%2Cdesc"

        all_detail_links = set()
        page_number = 1
        max_pages_to_crawl = 3

        print(f"\n--- BẮT ĐẦU CRAWL DANH SÁCH VIỆC LÀM ---")

        while page_number <= max_pages_to_crawl:
            parsed_url = urlparse(initial_listing_url)
            query_params = parse_qs(parsed_url.query)
            query_params['page'] = [str(page_number)]
            new_query_string = urlencode(query_params, doseq=True)
            current_page_url = urlunparse(parsed_url._replace(query=new_query_string))

            print(f"\n🔎 Đang truy cập trang danh sách: {current_page_url}")

            html_content = get_html_content(current_page_url)

            if html_content:
                detail_links_on_page = extract_detail_links(html_content)

                if not detail_links_on_page:
                    print(
                        f"Không tìm thấy liên kết chi tiết nào trên trang {page_number}. Có thể đã hết trang hoặc cấu trúc thay đổi.")
                    break

                new_links_count_on_page = 0
                for link in detail_links_on_page:
                    normalized_link = normalize_url(link)
                    if normalized_link not in all_detail_links:
                        all_detail_links.add(normalized_link)
                        new_links_count_on_page += 1

                print(
                    f"Tìm thấy {len(detail_links_on_page)} liên kết trên trang {page_number}. Thêm {new_links_count_on_page} liên kết mới vào danh sách tổng.")

                if new_links_count_on_page == 0 and page_number > 1:
                    print(
                        f"Không tìm thấy liên kết mới nào trên trang {page_number}. Có thể đã thu thập tất cả hoặc đến cuối.")
                    break

                page_number += 1
                time.sleep(random.uniform(2, 5))
            else:
                print(
                    f"Không thể lấy nội dung HTML từ '{current_page_url}' để phân tích liên kết. Dừng lại quá trình crawl danh sách.")
                break

        print(f"\n--- KẾT THÚC CRAWL DANH SÁCH VIỆC LÀM ---")

        if all_detail_links:
            print(
                f"\nTổng số {len(all_detail_links)} liên kết chi tiết duy nhất đã được tìm thấy qua tất cả các trang.")
            links_to_process = list(all_detail_links)
            random.shuffle(links_to_process)

            max_details_to_process = 10
            if len(links_to_process) > max_details_to_process:
                print(
                    f"Chỉ xử lý {max_details_to_process} liên kết chi tiết đầu tiên trong số {len(links_to_process)} tìm thấy (thay đổi 'max_details_to_process' để điều chỉnh).")
                links_to_process = links_to_process[:max_details_to_process]

            for i, link in enumerate(links_to_process):
                print(f"\n({i + 1}/{len(links_to_process)}) ")
                process_detail_link(link, collection)
                time.sleep(random.uniform(1, 3))

        else:
            print("Không tìm thấy bất kỳ liên kết chi tiết nào.")
    except Exception as e:
        print(f"❌ Lỗi tổng quát trong quá trình crawl hoặc kết nối MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("\n👋 Đã đóng kết nối MongoDB.")

    print("\nChương trình đã hoàn tất.")