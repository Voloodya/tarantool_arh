
#  Example
One day we needed to analyze the situation on the real estate market.
For this purpose, we decided to create a product that would analyze ads 
on multiple real-estate websites. At the same time, we wanted to be able
to subscribe to newsletters about new offers in the desired area.
So we needed a product that could:

- receive data from different sources,
- bring it to a general view,
- do analytics.

And Tarantool Data Grid was exactly the solution that helped us!

## Models
For the general view, we introduced three types of data:
1. Agent, the one who rents or sells real estate.
2. Real estate itself.
3. Address where the real estate is located.

Accordingly, these three types had the following fields:

1. Agent (Aggregate)
   - Name
   - Phone
2. Estate (Aggregate)
   - Action (sale, rent, rent per day)
   - Type (flat, room, house)
   - Renovation (decorating, European, etc.)
   - Floor space
   - Price
   - Address
3. Address (Value Object)
   - Country
   - City
   - Street
   - Building
   - District
   - Metro station

The following items were selected as parameters to analyze the area:
   - count of estates,
   - minimum, maximum and average price.

## Data handling
In our case, the data came from two different sources: `Favihome.Com` and `Estate Inc`.

Parser of `Favihome.Com` sent data in the following `JSON` formats:
- Agent:
  ```json
  {
    "home_id": 12345,
    "name": "John Smith",
    "phone": "+79998887766"
  }
  ```
- Estate:
  ```json
  {
    "home_id": 98765,
    "agent_uuid": "c3f731db-036b-4069-8de3-238c5bcf8df9",
    "action": "buy",
    "type": "flat",
    "square": 75,
    "price": 12000000,
    "address": {
      "country": "Russia",
      "city": "Moscow",
      "metro": "Aeroport",
      "district": "Aeroport"
    }
  }
  ```

Parser of `Estate Inc` sent data in the following `JSON` formats:
- Agent:
  ```json
  {
    "company": "estate inc",
    "agent": {
      "first_name": "John",
      "last_name": "Smith"
    }
  }
  ```
- Estate:
  ```json
  {
    "company": "estate inc",
    "estate": {
      "agent_uuid": "c3f731db-036b-4069-8de3-238c5bcf8df9",
      "action": "buy",
      "type": "flat",
      "renovation": "european",
      "price": 12000000,
      "street": "Baker Street",
      "building": "221B",
      "district": "Central"
    }
  }
  ```

Further on, these data were reduced to the general form (described above in
the [Models](#Models) section) and stored in the repository.

## Email newsletter
It is possible to subscribe to email notifications of new estates in the district.
To do this, you should send a `JSON` request with the following fields:
```json
{
  "type": "subscribe",
  "email": "test@example.com",
  "district": "Aeroport"
}
```

The distribution of emails looks like this:
1. Estate information is saved in a repository.
2. The same information falls into the `output_processor`.
3. In `output_processor`, emails are **immediately** sent to subscribers.

## Analytics
In order to find average prices and other useful information for each
district, you should use a GraphQL query similar to this one:
```graphql
{
  DistrictStat {
    district
    count
  }
}
```

The answer would look something like this:
```json
{
  "data": {
    "DistrictStat": [
      { "district": "Aeroport", "count": 175 },
      { "district": "Dinamo", "count": 285 }
    ]
  }
}
```

At the same time, this request received pre-calculated statistics.
Statistics were re-calculated in the following cases:

1. The task `update_all_districts_stat` started automatically every 
   five minutes to update statistics for all areas.
2. The function `calc_all_districts_stat` started manually to update 
   all statistics. For example:
   ```graphql
   {
     calc_all_districts_stat
   }
   ```
3. The function `calc_district_stat` started manually to update statistics 
   for a specific area. For example:
   ```graphql
   {
     calc_district_stat(district: "Aeroport")
   }
   ```

## Working with example

### Launching
1. At first you should run `TDG`. How to do this, you can see in README 
   in the root directory.
2. After that you need to apply config of example project. To do this 
   you should run `setconfig.py` with path to config folder as argument:
   ```bash
   ./setconfig.py example/config
   ```
3. `TDG` server is configured. To add random data to it, you can run
   generator script:
   ```bash
   example/generator.py -a 100 -e 1000 -t 1
   ```
   Flag `-a` indicates the count of agents to generate. 
   Flag `-e` indicates the count of estates to generate.
   Flag `-t` indicates the type of emulated resource (1 - Favihome.Com, 2 - Estate Inc)
4. Now you can open web interface (default: http://localhost:8080) and work in it.
   
### Getting statistics of districts
In web interface you can open `GraphQl` tab and send a GraphQl query to get statistics
for first 10 districts:
```graphql
{
 DistrictStat(first: 10) {
   district
   avg_price
 }
}
```
Also you can change set of getting fields. For example, you can add `count` field.
To make it easier to understand which fields you can add, you can look into 
`models.avsc` or simply press `Alt + Enter` in GraphQl editor to display hints.
**But maybe you will see an empty array of districts, it is normal**. 
This means that the regular task has not yet started after adding data. 
To manually start the statistics recalculation, use this graphql query:
```graphql
{
 calc_all_districts_stat
}
```
After that you can try to get statistics again.

### Email newsletter
Also you can subscribe to email notifications. But before that, you should to change 
the SMTP connection parameters in the `config.yml` (`connector.output` section) 
to your own and apply the config again, as described in the second paragraph.
Now you can open `Test` tab, select `JSON` request type at the top of the page 
and send one with the following fields:
```json
{
 "type": "subscribe",
 "email": "test@example.com",
 "district": "Aeroport"
}
```
That's all! Now entered email is subscribed to notifications of new estates 
in the selected district. To test it, you can manually add some estate. To do
that, first you should to get UUID of some agent. To do this make this GraphQl query:
```graphql
{
 Agent {
   uuid
 }
}
```
After that you can send `JSON` query similar to this one:
```json
{
 "company": "estate inc",
 "estate": {
   "agent_uuid": "HERE ENTER UUID OF AGENT",
   "action": "buy",
   "type": "flat",
   "renovation": "european",
   "price": 12000000,
   "street": "Baker Street",
   "building": "221B",
   "district": "Aeroport"
 }
}
```

### Writing a code
Let's edit input handler to support the third source `BestRent.ru`.
This source will send data in the following formats:
- Agent:
  ```json
  {
    "best_rent": "agent",
    "data": {
      "name": "John Smith"
    }
  }
  ```
- Estate:
  ```json
  {
    "best_rent": "estate",
    "data": {
      "agent_uuid": "c3f731db-036b-4069-8de3-238c5bcf8df9",
      "type": "flat",
      "price": 12000000,
      "address": {
        "city": "Moscow",
        "district": "Aeroport"
      }
    }
  }
  ```
Here we can see that all data have field `best_rent`.  It will be a unique parameter 
by which we will determine that the information came from this source. To add rule 
to classifier, open file [classifier.lua](/example/config/src/input/classifier.lua) and
add this lines:
```lua
if param.obj.best_rent ~= nil then
    param.routing_key = "best_rent_key"
    return param
end
```
Also we need to update handler for this data. Open file `handler.lua` in
[src/](/example/config/src) folder to edit. We need to detect a type of an object.
This can be done like this:
```lua
if param.obj.best_rent == "agent" then
    param.routing_key = "agent_key"
end

if param.obj.best_rent == "estate" then
    param.routing_key = "estate_key"
end
```
Also we need to write in `obj` field data of storage object. In our case it's easy to do:
```lua
param.obj = param.obj.data
```
In our storage, models must have uuid. To add uuid if it does not exist write these lines:
```lua
local uuid = require('uuid')
if param.obj.uuid == nil then
    param.obj.uuid = uuid.str()
end
```

Now we have modified the classifier and the handler to transform input object
into storage object.

Then we need to reapply config and after that we can test it!
Open web interface and send `JSON` query like this:
```json
{
  "best_rent": "agent",
  "data": {
    "uuid": "02dbf839-233d-4abe-8733-7e6739ca2bdb",
    "name": "John Smith"
  }
}
```
If everything is ok, then you will see "ok" response, and there will be no objects
in tab `Repair`. Also you can send this GraphQl query to check that returned name 
of agent is `John Smith`:
```graphql
{
  Agent(uuid: "02dbf839-233d-4abe-8733-7e6739ca2bdb") {
    name
  }
}
```
