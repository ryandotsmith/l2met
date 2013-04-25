package utils

import (
	"encoding/base64"
	"errors"
	"github.com/kr/fernet"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	OneHundredYears = time.Hour * 24 * 365 * 100
	keys            []*fernet.Key
)

func init() {
	if s := strings.Split(os.Getenv("SECRETS"), ":"); len(s) > 0 {
		keys = fernet.MustDecodeKeys(s...)
	}
}


func ParseAuth(r *http.Request) (string, string, error) {
	header, ok := r.Header["Authorization"]
	if !ok {
		return "", "", errors.New("Authorization header not set.")
	}
	authField := strings.SplitN(header[0], " ", 2)
	if len(authField) != 2 {
		return "", "", errors.New("Malformed header.")
	}
	authParts := strings.Split(authField[1], ":")

	auth, err := base64.StdEncoding.DecodeString(authParts[0])
	if err != nil {
		return authParts[0], "", err
	}
	//If we have gotten here, we have a signed, db-less authentication reque
	//If we can verify and decrypt, then we will pass the decrypted credenti
	//to the caller. Most of the time, the username and password will be
	//credentials to outlet providers. (e.g. Librato creds or Graphite creds
	//We care about the validity of those credentials here. If they are wron
	//the metrics will be dropped at the outlet. Keep an eye on http
	//authentication errors from the log output of the outlets.
	if len(keys) > 0 {
		if s := fernet.VerifyAndDecrypt(auth, OneHundredYears, keys); s != nil {
			parts := strings.Split(string(s), ":")
			return parts[0], parts[1], nil
		}
	}
	//If the user is not == "l2met" then dbless-auth is requested.
	//ATM we assume the first part (user field) contains a base64 encoded
	//representation of the outlet credentials.
	if len(auth) > 0 {
		decodedAuth, err := base64.StdEncoding.DecodeString(string(auth))
		if err != nil {
			return string(auth), "", err
		}
		outletCreds := strings.Split(string(decodedAuth), ":")
		//If the : is absent in parts[0], outletCreds[0] will contain the entire string in parts[0].
		user := outletCreds[0]
		//It is not required for the outletCreds to contain a pass.
		//The empty string that is returned will be handled by the outlet.
		var pass string
		if len(outletCreds) > 1 {
			pass = outletCreds[1]
		}
		return user, pass, nil
	}
	return "", "", errors.New("End of Authe chain reached.")
}
