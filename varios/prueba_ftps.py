'''
Created on Jun 1, 2020

@author: oirraza
'''

import ftplib
import ssl

class ReusedSslSocket(ssl.SSLSocket):
    def unwrap(self):
        pass


class ImplicitFTP_TLS(ftplib.FTP_TLS):
    """FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)  # reuses TLS session            
            conn.__class__ = ReusedSslSocket  # we should not close reused ssl socket when file transfers finish
        return conn, size



if __name__ == '__main__':
    host = 'ftps.pcr.com.ar'
    port = 990
    
    print('Ejecutando constructor...')
    ftp = ImplicitFTP_TLS()
#     ftp.set_debuglevel(2)
    ftp.encoding='utf-8'
    print('Ejecutando connect(host, port)...')
    ftp.connect(host=host, port=port)
    print('Ejecutando login(user, pass)...')
    ftp.login(user='AdminTri', passwd='M2nd4z1')
    print('Ejecutando prot_p()...')
    ftp.prot_p()
    print('Ejecutando dir...')
    ftp.dir()