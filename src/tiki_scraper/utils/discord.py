
import aiohttp
from ..config.settings import DISCORD_WEBHOOK_URL

async def send_discord_webhook(content=None, embed=None, wait_for_id=False):
    """
    Gửi thông báo về Discord via Webhook (Hỗ trợ Embeds)
    Nếu wait_for_id=True, trả về message_id để có thể edit sau
    """
    webhook_url = DISCORD_WEBHOOK_URL
    if not webhook_url:
        return None

    payload = {
        "username": "Tiki Scraper Bot 🤖",
    }
    
    if content:
        payload["content"] = content
    if embed:
        payload["embeds"] = [embed]
    
    try:
        # Thêm ?wait=true để Discord trả về message object
        url = f"{webhook_url}?wait=true" if wait_for_id else webhook_url
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, ssl=False) as response:
                if response.status not in [200, 204]:
                    print(f"⚠️ Discord Error: {response.status}")
                    return None
                if wait_for_id and response.status == 200:
                    data = await response.json()
                    return data.get("id")  # Trả về message ID
    except Exception as e:
        print(f"⚠️ Discord Fail: {e}")
    return None


async def edit_discord_message(message_id, embed=None):
    """
    Sửa một tin Discord đã gửi trước đó (dùng để update progress bar)
    """
    webhook_url = DISCORD_WEBHOOK_URL
    if not webhook_url or not message_id:
        return

    payload = {}
    if embed:
        payload["embeds"] = [embed]
    
    try:
        # URL để edit message: webhook_url/messages/message_id
        edit_url = f"{webhook_url}/messages/{message_id}"
        
        async with aiohttp.ClientSession() as session:
            async with session.patch(edit_url, json=payload, ssl=False) as response:
                if response.status not in [200, 204]:
                    print(f"⚠️ Discord Edit Error: {response.status}")
    except Exception as e:
        print(f"⚠️ Discord Edit Fail: {e}")
