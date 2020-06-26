Informations about this taps' schemas :

All these schemas represent the Ringover's data. Other endpoints exist, but are used for other purposes, such as POST requests.

The list of the endpoints that can be called without any additional data and that are used through GET requests (i.e., the ones that are implemented) :

- /blacklists/numbers (PROBLEM : endpoint contains a second slash, to be fixed)
- /presences
- /profiles
- /conversations (seems unused)
- /contacts
- /calls
- /conferences
- /tags
- /ivrs
- /numbers
- /users
- /groups
- /teams (this object contains data already retrieved through the other endpoints, i.e. numbers, ivrs and users)

The list of the GET endpoints that require additional data in the route to be called (hence, that don't have the need to be implemented) :

- /blacklists/numbers/{phone_number}
- /users/{user_id}/blacklists/numbers
- /users/{user_id}/blacklists/numbers/{phone_number}
- /users/{user_id}/presences
- /profiles/{profile_id}
- /conversations/{conv_id}/messages
- /conversations/{conv_id}
- /conversations/{conv_id}/members
- /contacts/{contact_id}
- /calls/{call_id}
- /conferences/{conference_id}
- /tags/{tag_id}
- /ivrs/{ivr_id}/scenarios/{scenario_id}
- /ivrs/{ivr_id}/scenarios
- /ivrs/{ivr_id}
- /numbers/{number}
- /users/{user_id}
- /users/{user_id}/plannings
- /users/{user_id}/presences
- /groups/{group_id}

The list of the API docs sections that are aren't used in this tap is the following :

- webhook (simulate an event trigger)
- telecoms (trigger a call between a caller and a recipient)
- push (send a SMS)
- channels (channels and calls manipulations)
