import base64
import hmac
import hashlib
import datetime


class ApiClient(object):
    def __init__(self, host, user=None, password=None):
        self.user = user
        self.password = password
        self.host = host
        self.now = datetime.datetime.now(datetime.timezone.utc)
        self.uri = None

    def _get_signature(self, uri, method='GET', content_md5=''):
        md5_password = hashlib.md5(self.password.encode("utf-8")).hexdigest()
        date = self.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
        sign_msg = "{}&{}&{}&{}".format(method, uri, date, content_md5)
        signature = base64.b64encode(
            hmac.new(
                md5_password.encode("utf-8"), sign_msg.encode("utf-8"),
                digestmod=hashlib.sha1
            ).digest()
        ).decode()
        return signature

    def _get_auth_token(self):
        if not self.uri:
            raise NotImplementedError(
                "ApiClient shall be implemented and the self.uri shall be assigned")
        signature = self._get_signature(self.uri)
        return "PeckShield {}:{}".format(self.user, signature)

    def get_headers(self):
        auth = self._get_auth_token()
        date = self.now.strftime("%a, %d %b %Y %H:%M:%S GMT")
        return {"Authorization": auth, "Date": date}
