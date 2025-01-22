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
                , WORKFLOW_NAME='IN_CBOT181B',WORKFLOW_ID='46bd9b4723254f789fd15f478bf942da', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT181B-0.4.prd-tz.1.1'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 14, 9, 26, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('46bd9b4723254f789fd15f478bf942da')

    cbot181bJob_vol = []
    cbot181bJob_volMnt = []
    cbot181bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot181bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot181bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot181bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot181bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot181bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5d09eac7cd944d99ad0ecf1652377fe3',
        'volumes': cbot181bJob_vol,
        'volume_mounts': cbot181bJob_volMnt,
        'env_from':cbot181bJob_env,
        'task_id':'cbot181bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.7',
        'arguments':["--job.name=cbot181bJob", "lobCd=IN", "startDate=20241101", "params=${YYYYMMDDHHMISS}"
],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


      
       
      
    cbot181bJob_verf = COMMON.getAgentVrfOperator({
        'id' : 'f3f02b6f5f724b4fad7cfee5e01cbaca',
        'task_id' : 'cbot181bJob_verf',
        'endpoint' : 'icis-cmmn-batchcommander-mz.icis.kt.co.kr',
        'data' : {
  "batchAgentUrl" : "https://icis-bill-batchagent.icis.kt.co.kr",
  "wflowVer" : "0.4.prd-tz.1.1",
  "wflowId" : "46bd9b4723254f789fd15f478bf942da",
  "wflowNm" : "IN_CBOT181B",
  "wflowTaskId" : "f3f02b6f5f724b4fad7cfee5e01cbaca",
  "taskId" : "cbot181bJob_verf",
  "cretDt" : "2025-01-14T00:29:22.998173Z",
  "cretId" : "91372241",
  "useSkip" : "Y",
  "vrfType" : ">",
  "vrfDiv1" : "QUERY",
  "vrfCmd1" : "SELECT COUNT(*)\nFROM IN_CWKO060RATECHNG\nWHERE 1=1\n AND sa_cd = '060A'\n;",
  "vrfDiv2" : "TEXT",
  "vrfCmd2" : "0"
},
        'taskAlrmSucesYn': 'N', # 성공 알림 전송
        'taskAlrmFailYn': 'N'  # 실패 알림 전송
    })

    Complete = COMMON.getICISCompleteWflowTask('46bd9b4723254f789fd15f478bf942da')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot181bJob,
        cbot181bJob_verf,
        Complete
    ]) 

    # authCheck >> cbot181bJob >> cbot181bJob_verf >> Complete
    workflow








