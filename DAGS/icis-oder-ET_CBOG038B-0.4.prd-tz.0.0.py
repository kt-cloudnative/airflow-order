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
                , WORKFLOW_NAME='ET_CBOG038B',WORKFLOW_ID='403d7d751bae4fd88a10264f83729e5f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG038B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 50, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('403d7d751bae4fd88a10264f83729e5f')

    cbog038bJob_vol = []
    cbog038bJob_volMnt = []
    cbog038bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbog038bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbog038bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog038bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog038bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog038bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6d1aba8a029a425cbbe7604459b102bb',
        'volumes': cbog038bJob_vol,
        'volume_mounts': cbog038bJob_volMnt,
        'env_from':cbog038bJob_env,
        'task_id':'cbog038bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=cbog038bJob", "reqYm=202411", "inputFile=kt_intbill_nochange_202412_20000.txt", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('403d7d751bae4fd88a10264f83729e5f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog038bJob,
        Complete
    ]) 

    # authCheck >> cbog038bJob >> Complete
    workflow








