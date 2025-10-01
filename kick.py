import sys
import time
import random
import datetime
import threading
import asyncio
import websockets
import json
import os
from threading import Thread
from streamlink import Streamlink
from threading import Semaphore
from fake_useragent import UserAgent
import tls_client

ua = UserAgent()
session = Streamlink()

CLIENT_TOKEN = "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823"
WS_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': ua.random,
    'sec-ch-ua': '"Chromium";v="137", "Google Chrome";v="137", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
session.set_option("http-headers", {
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": ua.random,
    "Client-ID": "ewvlchtxgqq88ru9gmfp1gmyt6h2b93",
    "Referer": "https://www.google.com/"
})

class ViewerBot:
    def __init__(self, nb_of_threads, channel_name):
        self.nb_of_threads = int(nb_of_threads)
        self.channel_name = self.extract_channel_name(channel_name)
        self.request_count = 0
        self.processes = []
        self.channel_url = "https://kick.com/" + self.channel_name
        self.thread_semaphore = Semaphore(int(nb_of_threads))
        self.active_threads = 0
        self.should_stop = False
        self.request_per_second = 0
        self.requests_in_current_second = 0
        self.last_request_time = time.time()
        self.status = {
            'state': 'initialized',
            'message': 'Bot initialized',
            'proxy_count': 0,
            'proxy_loading_progress': 0,
            'startup_progress': 0
        }
        self.stream_url_cache = None
        self.stream_url_last_updated = 0
        self.stream_url_lock = threading.Lock()
        self.stream_url_cache_duration = 0.5
        self.channel_id = None
        
        self._stats_lock = threading.Lock()
        self.open_websockets = 0
        self.websocket_attempts = 0
        self.live_streamer = False
        self.start_time = None
        self.stats_worker_thread = None
        self.pings_sent = 0
        self.heartbeats_sent = 0

    def get_channel_id(self):
        try:
            s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
            s.headers.update({
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://kick.com/',
                'Origin': 'https://kick.com',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            })
            
            try:
                response = s.get(f'https://kick.com/api/v2/channels/{self.channel_name}')
                if response.status_code == 200:
                    data = response.json()
                    self.channel_id = data.get("id")
                    return self.channel_id
            except Exception as e:
                pass
            
            try:
                response = s.get(f'https://kick.com/api/v1/channels/{self.channel_name}')
                if response.status_code == 200:
                    data = response.json()
                    self.channel_id = data.get("id")
                    return self.channel_id
            except Exception as e:
                pass
            
            try:
                response = s.get(f'https://kick.com/{self.channel_name}')
                if response.status_code == 200:
                    import re
                    patterns = [
                        r'"id":(\d+).*?"slug":"' + re.escape(self.channel_name) + r'"',
                        r'"channel_id":(\d+)',
                        r'channelId["\']:\s*(\d+)',
                        r'channel.*?id["\']:\s*(\d+)'
                    ]
                    
                    for pattern in patterns:
                        match = re.search(pattern, response.text, re.IGNORECASE)
                        if match:
                            self.channel_id = int(match.group(1))
                            return self.channel_id
            except Exception as e:
                pass
            
            print(f"All methods failed to get channel ID for: {self.channel_name}")
            return None
            
        except Exception as e:
            print(f"Error getting channel ID: {e}")
            return None

    def get_websocket_token(self):
        try:
            s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
            s.headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            })
            
            try:
                session_resp = s.get("https://kick.com")
                s.headers["X-CLIENT-TOKEN"] = CLIENT_TOKEN
                response = s.get('https://websockets.kick.com/viewer/v1/token')
                
                if response.status_code == 200:
                    data = response.json()
                    token = data.get("data", {}).get("token")
                    if token:
                        return token
            except Exception as e:
                pass
            
            token_endpoints = [
                'https://websockets.kick.com/viewer/v1/token',
                'https://kick.com/api/websocket/token',
                'https://kick.com/api/v1/websocket/token'
            ]
            
            for endpoint in token_endpoints:
                try:
                    s.headers["X-CLIENT-TOKEN"] = CLIENT_TOKEN
                    response = s.get(endpoint, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        token = data.get("data", {}).get("token") or data.get("token")
                        if token:
                            return token
                except Exception as e:
                    continue
            
            print("Failed to get WebSocket token from all endpoints")
            return None
            
        except Exception as e:
            print(f"Error getting WebSocket token: {e}")
            return None

    def extract_channel_name(self, input_str):
        if "kick.com/" in input_str:
            parts = input_str.split("kick.com/")
            channel = parts[1].split("/")[0].split("?")[0]
            return channel.lower()
        return input_str.lower()

    def _stats_worker(self):
        print()
        print()
        os.system('cls' if os.name == 'nt' else 'clear')
        while not self.should_stop:
            try:
                with self._stats_lock:
                    if self.start_time:
                        elapsed = datetime.datetime.now() - self.start_time
                        duration = f"{int(elapsed.total_seconds())}s"
                    else:
                        duration = "0s"
                    
                    open_ws = self.open_websockets
                    attempts = self.websocket_attempts
                    is_live = self.live_streamer
                    pings = self.pings_sent
                    heartbeats = self.heartbeats_sent
                

                print("\033[2A", end="")
                print(f"\033[2K\r[x] Open Websockets: \033[32m{open_ws}\033[0m | Websocket Attempts: \033[32m{attempts}\033[0m")
                print(f"\033[2K\r[x] Pings Sent: \033[32m{pings}\033[0m | Heartbeats Sent: \033[32m{heartbeats}\033[0m | Duration: \033[32m{duration}\033[0m")
                sys.stdout.flush()
                
                time.sleep(1)
            except Exception as e:
                time.sleep(1)

    def update_status(self, state, message, proxy_count=None, proxy_loading_progress=None, startup_progress=None):
        self.status.update({
            'state': state,
            'message': message,
            **(({'proxy_count': proxy_count} if proxy_count is not None else {})),
            **(({'proxy_loading_progress': proxy_loading_progress} if proxy_loading_progress is not None else {})),
            **(({'startup_progress': startup_progress} if startup_progress is not None else {}))
        })

    def get_url(self):
        current_time = time.time()
        
        with self.stream_url_lock:
            if (self.stream_url_cache and 
                current_time - self.stream_url_last_updated < self.stream_url_cache_duration):
                return self.stream_url_cache
            url = ""
            try:
                streams = session.streams(self.channel_url)
                if streams:
                    priorities = ['audio_only', '160p', '360p', '480p', '720p', '1080p', 'best', 'worst']
                    
                    for quality in priorities:
                        if quality in streams:
                            url = streams[quality].url
                            break
                    
                    if not url and streams:
                        quality = next(iter(streams))
                        url = streams[quality].url
                    
                    self.stream_url_cache = url
                    self.stream_url_last_updated = current_time
            except Exception as e:
                pass
            
            return url

    def stop(self):
        self.should_stop = True
        
        for thread in self.processes:
            if thread.is_alive():
                thread.join(timeout=1)
        
        self.processes.clear()
        self.active_threads = 0

    def open_url(self):
        self.send_websocket_view()

    def send_websocket_view(self):
        self.active_threads += 1
        with self._stats_lock:
            self.websocket_attempts += 1
        try:
            token = self.get_websocket_token()
            if not token:
                return
            
            if not self.channel_id:
                self.channel_id = self.get_channel_id()
                if not self.channel_id:
                    return

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._websocket_worker(token))
            except Exception as e:
                pass
            finally:
                try:
                    loop.close()
                except Exception:
                    pass
                    
        except Exception as e:
            pass
        finally:
            self.active_threads -= 1
            self.thread_semaphore.release()

    async def _websocket_worker(self, token):
        connection_opened = False
        try:
            ws_url = f"wss://websockets.kick.com/viewer/v1/connect?token={token}"
            
            async with websockets.connect(ws_url) as websocket:
                with self._stats_lock:
                    self.open_websockets += 1
                connection_opened = True
                
                handshake_msg = {
                    "type": "channel_handshake",
                    "data": {
                        "message": {"channelId": self.channel_id}
                    }
                }
                await websocket.send(json.dumps(handshake_msg))
                with self._stats_lock:
                    self.heartbeats_sent += 1
                
                ping_count = 0
                while not self.should_stop and ping_count < 10:
                    ping_count += 1
                    
                    ping_msg = {"type": "ping"}
                    await websocket.send(json.dumps(ping_msg))
                    with self._stats_lock:
                        self.pings_sent += 1
                    self.request_count += 1
                    
                    sleep_time = 12 + random.randint(1, 5)
                    await asyncio.sleep(sleep_time)
                
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            pass
        finally:
            if connection_opened:
                with self._stats_lock:
                    if self.open_websockets > 0:
                        self.open_websockets -= 1

    def main(self):
        start = datetime.datetime.now()
        self.start_time = start
        
        self.channel_id = self.get_channel_id()
        self.processes = []
        self.live_streamer = True
        
        self.stats_worker_thread = Thread(target=self._stats_worker, daemon=True)
        self.stats_worker_thread.start()
        
        while True:
            for i in range(0, int(self.nb_of_threads)):
                acquired = self.thread_semaphore.acquire()
                if acquired:
                    threaded = Thread(target=self.open_url)
                    self.processes.append(threaded)
                    threaded.daemon = True
                    threaded.start()
                    
                    time.sleep(0.35)

            if self.should_stop:
                for _ in range(self.nb_of_threads):
                    try:
                        self.thread_semaphore.release()
                    except ValueError:
                        pass
                break

        for t in self.processes:
            t.join()


if __name__ == "__main__":
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
        channel = input("Enter channel name or URL: ").strip()
        if not channel:
            print("Channel name is needed.")
            sys.exit(1)
            
        while True:
            try:
                threads = int(input("Enter number of viewers: ").strip())
                if threads > 0:
                    break
                else:
                    print("Number of threads must be bigger than 0")
            except ValueError:
                print("Please enter a valid number")
        

        bot = ViewerBot(
            nb_of_threads=threads,
            channel_name=channel
        )
        bot.main()
    except KeyboardInterrupt:
        if 'bot' in locals():
            bot.stop()
        sys.exit(0)
