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
                , WORKFLOW_NAME='EI_CBOT501B',WORKFLOW_ID='a2342b9f48784265aaf83dbc756a848d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT501B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 15, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a2342b9f48784265aaf83dbc756a848d')

    cbot501bJob_vol = []
    cbot501bJob_volMnt = []
    cbot501bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot501bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot501bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbot501bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot501bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot501bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8bccf944b5f94031ba680165d0edff56',
        'volumes': cbot501bJob_vol,
        'volume_mounts': cbot501bJob_volMnt,
        'env_from':cbot501bJob_env,
        'task_id':'cbot501bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.2',
        'arguments':["--job.name=cbot501bJob", "requestDate="+str(datetime.now()), "procMonth=202407", "progName=cbot501b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a2342b9f48784265aaf83dbc756a848d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot501bJob,
        Complete
    ]) 

    # authCheck >> cbot501bJob >> Complete
    workflow








