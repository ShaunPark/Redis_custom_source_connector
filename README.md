#  Redis Custom Connector
- REDIS의 sortedSet에서 score가 현재 시간(Java의 System.currentTimeMillis() 기준)이전이 데이터를 조회해서 지정된 토픽으로 생성
- 토픽에 데이터가 저장되면 해당 데이터를 REDIS sortedSet에서 삭제
- 조회 주기는 최소 10초(10000ms)
- 중복 전송을 피하기 위해 조회된 데이터는 내부적으로 관리되며, 토픽에 저장된 후 내부 목록에서 삭제됨
- REDIS가 연결에 실패한 경우, 다음 poll시점에 다시연결하여 처리

## 설정 값
[Config Image](./static/config.png?raw=true "Config")
- Poll Interval : poll 주기, 기본값 10초, 최소 10초
- Redis Server Hostname : redis host 명. connect 클러스터에서 접근 가능한 호스트 명 혹은 아이피. 기본값 'localhost'
- Redis Server Port Number : redis port 번호. 기본값 6379
- Redis sorted set key : 조회하고자 하는 sortedset을 찾기 위한 key 값
- Topic name to produce : 조회된 데이터를 저장할 토픽 명
- Batch Size : redis에서 데이터를 조회해 오는 단위. 기본값 2000.