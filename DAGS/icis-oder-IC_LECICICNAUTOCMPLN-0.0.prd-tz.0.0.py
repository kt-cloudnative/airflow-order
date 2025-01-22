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
                , WORKFLOW_NAME='IC_LECICICNAUTOCMPLN',WORKFLOW_ID='72e6e3a1405b455b9c2206a19d3fc6a7', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_LECICICNAUTOCMPLN-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 36, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('72e6e3a1405b455b9c2206a19d3fc6a7')

    leciCicnAutoCmplnJob_vol = []
    leciCicnAutoCmplnJob_volMnt = []
    leciCicnAutoCmplnJob_vol.append(getVolume('shared-volume','shared-volume'))
    leciCicnAutoCmplnJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    leciCicnAutoCmplnJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    leciCicnAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    leciCicnAutoCmplnJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    leciCicnAutoCmplnJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b2cfd95a52a14cadaf107b011e9bee41',
        'volumes': leciCicnAutoCmplnJob_vol,
        'volume_mounts': leciCicnAutoCmplnJob_volMnt,
        'env_from':leciCicnAutoCmplnJob_env,
        'task_id':'leciCicnAutoCmplnJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.6',
        'arguments':["--job.name=leciCicnAutoCmplnJob", "reqDate=${YYYYMMDDHHMISS}", "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('72e6e3a1405b455b9c2206a19d3fc6a7')

    workflow = COMMON.getICISPipeline([
        authCheck,
        leciCicnAutoCmplnJob,
        Complete
    ]) 

    # authCheck >> leciCicnAutoCmplnJob >> Complete
    workflow








