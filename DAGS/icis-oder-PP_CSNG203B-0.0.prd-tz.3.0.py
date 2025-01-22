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
                , WORKFLOW_NAME='PP_CSNG203B',WORKFLOW_ID='3185c2ab1cc94ceea1a6718b02da09db', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG203B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3185c2ab1cc94ceea1a6718b02da09db')

    csng203bJob_vol = []
    csng203bJob_volMnt = []
    csng203bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng203bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng203bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng203bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng203bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng203bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '673a8c97a33640b8a7b3c6eaaace3e02',
        'volumes': csng203bJob_vol,
        'volume_mounts': csng203bJob_volMnt,
        'env_from':csng203bJob_env,
        'task_id':'csng203bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng203bJob", "workMonth=202406", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3185c2ab1cc94ceea1a6718b02da09db')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng203bJob,
        Complete
    ]) 

    # authCheck >> csng203bJob >> Complete
    workflow








