from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='IN_CBOT182B',WORKFLOW_ID='5509d245cbc143109481df7c324230d5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT182B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 6, 17, 24, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5509d245cbc143109481df7c324230d5')

    cbot182bJob_vol = []
    cbot182bJob_volMnt = []
    cbot182bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot182bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot182bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot182bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot182bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot182bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '30e40ff67804464dad4bffd94f03d61a',
        'volumes': cbot182bJob_vol,
        'volume_mounts': cbot182bJob_volMnt,
        'env_from':cbot182bJob_env,
        'task_id':'cbot182bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.4.0.78',
        'arguments':["--job.name=cbot182bJob", "lobCd=IN", "params=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


      
       
      
    cbot182b_verf = COMMON.getAgentVrfOperator({
        'id' : '8e296a5779ad4388878efad698cad385',
        'task_id' : 'cbot182b_verf',
        'endpoint' : 'icis-cmmn-batchcommander-mz.icis.kt.co.kr',
        'data' : {
  "batchAgentUrl" : "https://icis-oder-batchagent.icis.kt.co.kr",
  "wflowVer" : "0.4.prd-tz.0.0",
  "wflowId" : "5509d245cbc143109481df7c324230d5",
  "wflowNm" : "IN_CBOT182B",
  "wflowTaskId" : "8e296a5779ad4388878efad698cad385",
  "taskId" : "cbot182b_verf",
  "cretDt" : "2025-01-06T08:24:49.994468Z",
  "cretId" : "91360259",
  "useSkip" : "Y",
  "vrfType" : "==",
  "vrfDiv1" : "FILE",
  "vrfCmd1" : "/app/order/in/cbot182b/",
  "vrfDiv2" : "TEXT",
  "vrfCmd2" : "true"
},
        'taskAlrmSucesYn': 'N', # 성공 알림 전송
        'taskAlrmFailYn': 'N'  # 실패 알림 전송
    })

    Complete = COMMON.getICISCompleteWflowTask('5509d245cbc143109481df7c324230d5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot182bJob,
        cbot182b_verf,
        Complete
    ]) 

    # authCheck >> cbot182bJob >> cbot182b_verf >> Complete
    workflow








