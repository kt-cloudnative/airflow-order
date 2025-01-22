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
                , WORKFLOW_NAME='IC_CSNJ187B',WORKFLOW_ID='07dfb1f6543e4fc0b94166645afe91b8', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CSNJ187B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 15, 23, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('07dfb1f6543e4fc0b94166645afe91b8')

    csnj187bJob_vol = []
    csnj187bJob_volMnt = []
    csnj187bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj187bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj187bJob_env = [getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISConfigMap('icis-oder-infocomm-batch-configmap2'), getICISSecret('icis-oder-infocomm-batch-secret')]
    csnj187bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnj187bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj187bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a5360ab5236c4a5c8fd50184ba0d58cc',
        'volumes': csnj187bJob_vol,
        'volume_mounts': csnj187bJob_volMnt,
        'env_from':csnj187bJob_env,
        'task_id':'csnj187bJob',
        'image':'/icis/icis-oder-infocomm-batch:0.4.1.33',
        'arguments':["--job.name=csnj187bJob", "requestDate="+str(datetime.now()), "procMonth=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('07dfb1f6543e4fc0b94166645afe91b8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj187bJob,
        Complete
    ]) 

    # authCheck >> csnj187bJob >> Complete
    workflow








