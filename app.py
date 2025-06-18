import requests
import time
import random  # Để ngẫu nhiên hóa thời gian chờ
from bs4 import BeautifulSoup
from pymongo import MongoClient  # Import thư viện PyMongo
import re  # Import re module để làm sạch chuỗi

# --- Cấu hình MongoDB ---
# Đảm bảo MongoDB server đang chạy trên máy của bạn (mặc định là localhost:27017)
# ĐÃ CẬP NHẬT CHUỖI KẾT NỐI MONGODB THEO YÊU CẦU CỦA BẠN
MONGO_URI = "mongodb+srv://lumconon0911:SWD-SWD@cluster.slolqwf.mongodb.net/"
DATABASE_NAME = "topdev_jobs_db"  # Tên database cho dữ liệu TopDev
COLLECTION_NAME = "job_details"  # Tên collection để lưu chi tiết công việc


def has_all_classes(tag, class_string):
    """
    Checks if a BeautifulSoup tag has all classes specified in a space-separated string.

    Args:
        tag (bs4.Tag): The BeautifulSoup tag to check.
        class_string (str): A space-separated string of classes to look for.

    Returns:
        bool: True if the tag has all specified classes, False otherwise.
    """
    if not tag or not tag.has_attr('class'):
        return False
    target_classes_list = class_string.split()
    # Check if all target classes are present in the tag's class list
    return all(cls in tag['class'] for cls in target_classes_list)


def get_html_content(url):
    """
    Tải xuống nội dung HTML của một trang web và trả về nó.

    Args:
        url (str): Địa chỉ URL của trang web cần tải.

    Returns:
        str: Nội dung HTML của trang web, hoặc None nếu có lỗi.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)  # Added timeout
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải trang web '{url}': {e}")
        return None


def extract_detail_links(html_content):
    """
    Phân tích cú pháp nội dung HTML để trích xuất các liên kết chi tiết
    từ một container cụ thể và loại bỏ trùng lặp.

    Args:
        html_content (str): Nội dung HTML của trang web.

    Returns:
        set: Một tập hợp các URL chi tiết duy nhất.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    unique_detail_links = set()

    # The new target class for the listing page container
    target_classes_listing_page = "grid grid-cols-1 gap-y-2 lg:gap-y-2.5"

    # Use lambda with has_all_classes to robustly find the container div
    container = soup.find('div',
                          class_=lambda c: has_all_classes(BeautifulSoup(f'<div class="{c}"></div>', 'html.parser').div,
                                                           target_classes_listing_page) if c else False)

    if container:
        links = container.find_all('a', href=True)
        for link in links:
            href = link['href']
            # Ensure links are absolute. Base URL for vieclam24h.vn.
            if href.startswith('http'):
                unique_detail_links.add(href)
            else:
                base_url = "https://vieclam24h.vn"
                full_link = f"{base_url}{href}"
                unique_detail_links.add(full_link)
    else:
        print(f"Không tìm thấy container với các class: '{target_classes_listing_page}'.")

    return unique_detail_links


def extract_section_content(soup_obj, heading_text):
    """
    Tìm một tiêu đề (h2, h3, hoặc span với class cụ thể) và trích xuất nội dung từ các phần tử kế tiếp.
    Tìm kiếm không phân biệt chữ hoa/thường cho heading_text.
    """
    # Convert the search heading text to lowercase for case-insensitive comparison
    lower_heading_text = heading_text.lower()

    heading_tag = None

    # Try to find heading in h2, h3, or span
    for tag_name in ['h2', 'h3', 'span']:
        heading_tag = soup_obj.find(tag_name, string=lambda text: text and lower_heading_text in text.lower())
        if heading_tag:
            break  # Found a heading, exit loop

    if heading_tag:
        content_element = None

        # User-specified class for content sections that often contain the details
        specific_content_class = "jsx-5b2773f86d2f74b mb-2 text-14 break-words text-se-neutral-80 text-description"

        # Priority 1: Find div with class "prose" as next sibling (common pattern)
        content_element = heading_tag.find_next_sibling('div', class_='prose')

        # Priority 2: If not found, try the specific user-provided class for div/span as next sibling
        if not content_element:
            # Need to search for both div and span with this complex class
            for tag_type in ['div', 'span']:
                # Create a dummy tag to use has_all_classes helper
                dummy_tag_html = f'<{tag_type} class="">content</{tag_type}>'

                potential_content = heading_tag.find_next_sibling(tag_type, class_=lambda c: has_all_classes(
                    BeautifulSoup(dummy_tag_html, 'html.parser').find(tag_type),
                    specific_content_class) if c else False)
                if potential_content:
                    content_element = potential_content
                    break  # Found it, break from tag_type loop

        # Priority 3: If still not found, try common list/paragraph/div/span containers as next sibling
        if not content_element:
            content_element = heading_tag.find_next_sibling(['div', 'ul', 'ol', 'p', 'span'])

        if content_element:
            # Get raw text, replace newlines with space, then normalize all whitespace
            raw_text = content_element.get_text(separator=' ', strip=False)
            cleaned_text = re.sub(r'\s+', ' ', raw_text).strip()
            return cleaned_text
        else:
            # Fallback: If no suitable sibling element is found,
            # return the text of the heading itself (if it contains more than just the heading text)
            raw_heading_text = heading_tag.get_text(separator=' ', strip=False)
            cleaned_heading_text = re.sub(r'\s+', ' ', raw_heading_text).strip()

            # If the cleaned heading text is just the heading itself, means no distinct content was found
            if cleaned_heading_text.lower() == lower_heading_text:
                return None  # Return None if no actual content found, making it cleaner for checks
            else:
                return cleaned_heading_text

    return None  # Return None if heading not found.


def crawl_topdev_simple():
    """
    Thực hiện crawl trang danh sách việc làm IT trên TopDev.vn,
    trích xuất URL chi tiết, truy cập từng URL và lưu vào MongoDB.
    Không sử dụng GPMlogin, dịch vụ anti-captcha.
    """
    client = None  # Khởi tạo client bên ngoài để đảm bảo đóng kết nối
    try:
        # Kết nối đến MongoDB
        client = MongoClient(MONGO_URI)
        # Ping the server to ensure a successful connection
        client.admin.command('ping')
        print(f"✅ Đã kết nối thành công tới MongoDB Atlas cluster.")
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        print(f"Đang sử dụng database '{DATABASE_NAME}' và collection '{COLLECTION_NAME}'.")

        list_url = "https://topdev.vn/jobs/search?job_categories_ids=2"
        print(f"🔎 Đang truy cập trang danh sách: {list_url}")

        # Cấu hình User-Agent để giống trình duyệt thật hơn
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        try:
            response = requests.get(list_url, headers=headers, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"❌ Lỗi khi truy cập trang danh sách {list_url}: {e}")
            return

        list_soup = BeautifulSoup(response.text, 'html.parser')

        job_detail_urls = []
        job_item_containers = list_soup.find_all('div',
                                                 class_='relative rounded border border-solid transition-all hover:shadow-md border-primary bg-primary-100')

        if not job_item_containers:
            print("❌ Không tìm thấy container việc làm nào trên trang danh sách với bộ chọn đã định.")
            return

        print(f"Đã tìm thấy {len(job_item_containers)} container việc làm trên trang danh sách.")

        for container in job_item_containers:
            parent_a_tag = container.find_parent('a', href=True)
            if parent_a_tag:
                detail_url = parent_a_tag.get('href')
                if detail_url:
                    if not detail_url.startswith('http'):
                        detail_url = f"https://topdev.vn{detail_url}"
                    job_detail_urls.append(detail_url)

        job_detail_urls = list(set(job_detail_urls))

        if not job_detail_urls:
            print("❌ Không tìm thấy URL chi tiết việc làm nào sau khi trích xuất và lọc trùng lặp.")
            return

        print(f"Đã trích xuất {len(job_detail_urls)} URL chi tiết (đã loại trùng lặp).")

        # --- LẶP QUA TỪNG URL CHI TIẾT VÀ TRÍCH XUẤT, LƯU VÀO MONGODB ---
        for i, detail_url in enumerate(job_detail_urls):
            print(f"\n--- [{i + 1}/{len(job_detail_urls)}] Đang truy cập trang chi tiết: {detail_url} ---")
            try:
                detail_response = requests.get(detail_url, headers=headers, timeout=10)
                detail_response.raise_for_status()

                detail_soup = BeautifulSoup(detail_response.text, 'html.parser')

                # Trích xuất tiêu đề công việc (ví dụ từ thẻ <title> của trang)
                job_title = detail_soup.find('title').get_text(strip=True) if detail_soup.find(
                    'title') else "Không có tiêu đề."

                # Trích xuất nội dung cho từng phần
                # Attempt to get content, which will be None if not found
                responsibilities_content = extract_section_content(detail_soup,
                                                                   "Responsibilities") or extract_section_content(
                    detail_soup, "Trách nhiệm công việc")
                requirements_content = extract_section_content(detail_soup, "Requirements") or extract_section_content(
                    detail_soup, "Kỹ năng & Chuyên môn")
                benefits_content = extract_section_content(detail_soup, "Benefits") or extract_section_content(
                    detail_soup, "phúc lợi dành cho bạn")

                # --- KIỂM TRA ĐIỀU KIỆN LƯU VÀO DATABASE ---
                # Chỉ lưu vào database nếu ít nhất một trong ba phần chính có nội dung
                if not responsibilities_content and not requirements_content and not benefits_content:
                    print(
                        f"⚠️ Bỏ qua lưu vào database cho '{job_title}' ({detail_url}). Không tìm thấy nội dung Responsibilities, Requirements, hoặc Benefits.")
                    time.sleep(random.uniform(1, 3))  # Vẫn tạm dừng để tránh quá tải
                    continue  # Chuyển sang URL tiếp theo

                # Chuẩn bị dữ liệu để lưu vào MongoDB
                job_data = {
                    "url": detail_url,
                    "title": job_title,
                    "responsibilities": responsibilities_content,  # Sẽ là None nếu không tìm thấy
                    "requirements": requirements_content,  # Sẽ là None nếu không tìm thấy
                    "benefits": benefits_content,  # Sẽ là None nếu không tìm thấy
                    "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S")  # Thời gian crawl
                }

                # Lưu dữ liệu vào MongoDB (upsert để tránh trùng lặp)
                # Nếu 'url' đã tồn tại, cập nhật bản ghi đó. Nếu không, chèn bản ghi mới.
                result = collection.update_one(
                    {'url': job_data['url']},  # Filter để tìm bản ghi
                    {'$set': job_data},  # Dữ liệu để cập nhật/chèn
                    upsert=True  # Nếu không tìm thấy, tạo mới
                )

                if result.upserted_id:
                    print(f"✅ Đã thêm mới dữ liệu cho '{job_data['title']}' vào MongoDB (ID: {result.upserted_id}).")
                elif result.modified_count > 0:
                    print(f"✅ Đã cập nhật dữ liệu cho '{job_data['title']}' trong MongoDB.")
                else:
                    print(f"ℹ️ Dữ liệu cho '{job_data['title']}' đã tồn tại và không có thay đổi.")

                # In dữ liệu đã trích xuất ra console để kiểm tra
                print("\n" + "=" * 50)
                print(f"DỮ LIỆU TRÍCH XUẤT TỪ TRANG CHI TIẾT ({detail_url}):")
                print("=" * 50 + "\n")
                print(f"Tiêu đề: {job_title}")
                # Hiển thị "Không tìm thấy." nếu nội dung là None
                print(
                    f"1. Responsibilities: {responsibilities_content if responsibilities_content else 'Không tìm thấy.'}")
                print(f"2. Requirements: {requirements_content if requirements_content else 'Không tìm thấy.'}")
                print(f"3. Benefits: {benefits_content if benefits_content else 'Không tìm thấy.'}")
                print("\n" + "=" * 50)

                # Tạm dừng ngẫu nhiên để tránh bị chặn IP
                time.sleep(random.uniform(1, 3))
            except requests.exceptions.RequestException as e:
                print(f"❌ Lỗi khi truy cập trang chi tiết {detail_url}: {e}")
                continue
            except Exception as e:
                print(f"❌ Lỗi khi xử lý dữ liệu hoặc lưu vào MongoDB cho {detail_url}: {e}")
                continue

    except Exception as e:
        print(f"❌ Lỗi tổng quát trong quá trình crawl hoặc kết nối MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("\n👋 Đã đóng kết nối MongoDB.")

    print("\nHoàn tất quá trình crawl các link chi tiết và lưu dữ liệu vào MongoDB.")


if __name__ == "__main__":
    crawl_topdev_simple()
    print("\nChương trình đã hoàn tất.")
