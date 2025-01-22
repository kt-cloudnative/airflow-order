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
                , WORKFLOW_NAME='IC_CSNJ922B',WORKFLOW_ID='8e698605375947ef90a95f4c7293a1a6', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ922B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 15, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8e698605375947ef90a95f4c7293a1a6')

    csnj922bJob_vol = []
    csnj922bJob_volMnt = []
    csnj922bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj922bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj922bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnj922bJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    csnj922bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj922bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6d5a4c5e90c240f297ac00e7ff4d391e',
        'volumes': csnj922bJob_vol,
        'volume_mounts': csnj922bJob_volMnt,
        'env_from':csnj922bJob_env,
        'task_id':'csnj922bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.5',
        'arguments':["--job.name=csnj922bJob", "requestDate=${YYYYMMDDHHMISS}", "gszProcMonth=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8e698605375947ef90a95f4c7293a1a6')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj922bJob,
        Complete
    ]) 

    # authCheck >> csnj922bJob >> Complete
    workflow








