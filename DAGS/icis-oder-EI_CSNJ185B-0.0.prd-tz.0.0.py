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
                , WORKFLOW_NAME='EI_CSNJ185B',WORKFLOW_ID='fe63ba7b13ee4568990fcd3b1740ec3e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CSNJ185B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('fe63ba7b13ee4568990fcd3b1740ec3e')

    csnj185bJob_vol = []
    csnj185bJob_volMnt = []
    csnj185bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnj185bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnj185bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnj185bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    csnj185bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnj185bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '22db7d08437c495a863d7c7df5b77cd5',
        'volumes': csnj185bJob_vol,
        'volume_mounts': csnj185bJob_volMnt,
        'env_from':csnj185bJob_env,
        'task_id':'csnj185bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.33',
        'arguments':["--job.name=csnj185bJob", "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('fe63ba7b13ee4568990fcd3b1740ec3e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnj185bJob,
        Complete
    ]) 

    # authCheck >> csnj185bJob >> Complete
    workflow








