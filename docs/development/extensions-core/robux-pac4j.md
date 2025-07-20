---
id: robux-pac4j
title: "Robux pac4j based Security extension"
---

<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->


Apache Robux Extension to enable [OpenID Connect](https://openid.net/connect/) based Authentication for Robux Processes using [pac4j](https://github.com/pac4j/pac4j) as the underlying client library.
This can be used  with any authentication server that supports same e.g. [Okta](https://developer.okta.com/).
The pac4j authenticator should only be used at the router node to enable a group of users in existing authentication server to interact with Robux cluster, using the [web console](../../operations/web-console.md). 

This extension also provides a JWT authenticator that validates [ID Tokens](https://openid.net/specs/openid-connect-core-1_0.html#CodeIDToken) associated with a request. ID Tokens are attached to the request under the `Authorization` header with the bearer token prefix - `Bearer `. This authenticator is intended for services to talk to Robux by initially authenticating with an OIDC server to retrieve the ID Token which is then attached to every Robux request.

This extension does not support JDBC client authentication.

## Configuration

### Creating an Authenticator
```
#Create a pac4j web user authenticator
robux.auth.authenticatorChain=["pac4j"]
robux.auth.authenticator.pac4j.type=pac4j

#Create a JWT token authenticator
robux.auth.authenticatorChain=["jwt"]
robux.auth.authenticator.jwt.type=jwt
```

### Properties
|Property|Description|Default|required|
|--------|---------------|-----------|-------|
|`robux.auth.pac4j.cookiePassphrase`|Passphrase for encrypting the cookies used to manage authentication session with browser. It can be provided as plaintext string or the (recommended) [Password Provider](../../operations/password-provider.md).|none|Yes|
|`robux.auth.pac4j.readTimeout`|Socket connect and read timeout duration used when communicating with authentication server|PT5S|No|
|`robux.auth.pac4j.enableCustomSslContext`|Whether to use custom SSLContext setup via [simple-client-sslcontext](simple-client-sslcontext.md) extension which must be added to extensions list when this property is set to true.|false|No|
|`robux.auth.pac4j.oidc.clientID`|OAuth Client Application id.|none|Yes|
|`robux.auth.pac4j.oidc.clientSecret`|OAuth Client Application secret. It can be provided as plaintext string or The [Password Provider](../../operations/password-provider.md).|none|Yes|
|`robux.auth.pac4j.oidc.discoveryURI`|discovery URI for fetching OP metadata [see this](http://openid.net/specs/openid-connect-discovery-1_0.html).|none|Yes|
|`robux.auth.pac4j.oidc.oidcClaim`|[claim](https://openid.net/specs/openid-connect-core-1_0.html#Claims) that will be extracted from the ID Token after validation.|name|No|
|`robux.auth.pac4j.oidc.scope`| scope is used by an application during authentication to authorize access to a user's details.|`openid profile email`|No|

:::info
Users must set a strong passphrase to ensure that an attacker is not able to guess it simply by brute force.
A compromised passphrase may allow an attacker to read and manipulate session cookies.
For more details, see [CVE-2024-45384](https://nvd.nist.gov/vuln/detail/CVE-2024-45384).
:::