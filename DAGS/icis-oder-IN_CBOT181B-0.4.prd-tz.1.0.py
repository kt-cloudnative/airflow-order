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
                , WORKFLOW_NAME='IN_CBOT181B',WORKFLOW_ID='e5e1c50fb13b4a8394bfbca4c54cbd7b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT181B-0.4.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 6, 17, 23, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e5e1c50fb13b4a8394bfbca4c54cbd7b')

    cbot181bJob_vol = []
    cbot181bJob_volMnt = []
    cbot181bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot181bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot181bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot181bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot181bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot181bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '00a57a8254a644149125b76a9326dd61',
        'volumes': cbot181bJob_vol,
        'volume_mounts': cbot181bJob_volMnt,
        'env_from':cbot181bJob_env,
        'task_id':'cbot181bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.7',
        'arguments':["--job.name=cbot181bJob", "lobCd=IN", "startDate=${YYYYMM, MM, -1}01", "params=${YYYYMMDDHHMISS}"
],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      


      
       
      
    cbot181bJob_verf = COMMON.getAgentVrfOperator({
        'id' : '335f2bcfc9094eec83cf5da15e225679',
        'task_id' : 'cbot181bJob_verf',
        'endpoint' : 'icis-cmmn-batchcommander-mz.icis.kt.co.kr',
        'data' : {
  "batchAgentUrl" : "https://icis-bill-batchagent.icis.kt.co.kr",
  "wflowVer" : "0.4.prd-tz.1.0",
  "wflowId" : "e5e1c50fb13b4a8394bfbca4c54cbd7b",
  "wflowNm" : "IN_CBOT181B",
  "wflowTaskId" : "335f2bcfc9094eec83cf5da15e225679",
  "taskId" : "cbot181bJob_verf",
  "cretDt" : "2025-01-10T05:14:18.563827Z",
  "cretId" : "91360259",
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

    Complete = COMMON.getICISCompleteWflowTask('e5e1c50fb13b4a8394bfbca4c54cbd7b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot181bJob,
        cbot181bJob_verf,
        Complete
    ]) 

    # authCheck >> cbot181bJob >> cbot181bJob_verf >> Complete
    workflow








